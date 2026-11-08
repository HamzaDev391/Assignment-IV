import socket
import json
import random
import time
import threading

# Configuration
CHANNEL_HOST = "127.0.0.1"
CHANNEL_PORT = 5000

RECEIVER_HOST = "127.0.0.1"
RECEIVER_PORT = 5001

LOSS_PROBABILITY = 0.20

MIN_DELAY = 0.2
MAX_DELAY = 1.5


def receive_messages(sock):
    """Read newline-delimited JSON messages."""

    buffer = b""

    while True:
        data = sock.recv(4096)

        if not data:
            break

        buffer += data

        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)

            if line:
                yield json.loads(line.decode())


def send_message(sock, message):
    """Send a JSON message over TCP."""

    data = json.dumps(message).encode() + b"\n"
    sock.sendall(data)


class UnreliableChannel:

    def __init__(self):

        self.server_socket = None
        self.receiver_socket = None

        self.send_lock = threading.Lock()

        self.pending_lock = threading.Lock()
        self.pending = 0

    def forward_packet(self, packet):

        try:
            delay = random.uniform(MIN_DELAY, MAX_DELAY)

            if delay > 0:
                print(
                    f"[Channel] Delaying packet "
                    f"{packet['seq']} by {delay:.2f}s"
                )

                time.sleep(delay)

            # Synchronize writes to the receiver socket.
            with self.send_lock:
                send_message(self.receiver_socket, packet)

            print(
                f"[Channel] Forwarded packet {packet['seq']}"
            )

        except OSError as error:
            print(f"[Channel] Forwarding error: {error}")

        finally:
            with self.pending_lock:
                self.pending -= 1

    def run(self):

        # Connect to receiver.
        while True:

            try:
                self.receiver_socket = socket.socket(
                    socket.AF_INET,
                    socket.SOCK_STREAM
                )

                self.receiver_socket.connect(
                    (RECEIVER_HOST, RECEIVER_PORT)
                )

                break

            except ConnectionRefusedError:
                self.receiver_socket.close()
                time.sleep(0.2)

        print("[Channel] Connected to receiver")

        # Create server socket for sender.
        self.server_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        self.server_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        self.server_socket.bind(
            (CHANNEL_HOST, CHANNEL_PORT)
        )

        self.server_socket.listen(1)

        print(f"[Channel] Listening on port {CHANNEL_PORT}")

        sender_socket, address = self.server_socket.accept()

        print(f"[Channel] Sender connected: {address}")

        # Process incoming packets.
        for packet in receive_messages(sender_socket):

            if packet["type"] == "FIN":

                # Wait for previously delayed packets.
                while True:

                    with self.pending_lock:
                        if self.pending == 0:
                            break

                    time.sleep(0.05)

                with self.send_lock:
                    send_message(self.receiver_socket, packet)

                print("[Channel] Forwarded termination message")
                break

            seq = packet["seq"]

            # Simulate packet loss.
            if random.random() < LOSS_PROBABILITY:

                print(f"[Channel] DROPPED packet {seq}")
                continue

            # Process each surviving packet independently.
            with self.pending_lock:
                self.pending += 1

            thread = threading.Thread(
                target=self.forward_packet,
                args=(packet,)
            )

            thread.start()

        sender_socket.close()
        self.receiver_socket.close()
        self.server_socket.close()

        print("[Channel] Shutdown complete.")


if __name__ == "__main__":
    channel = UnreliableChannel()
    channel.run()
