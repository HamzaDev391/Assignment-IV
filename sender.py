import socket
import json
import time
import threading

# Configuration
SENDER_HOST = "127.0.0.1"
ACK_PORT = 5002

CHANNEL_HOST = "127.0.0.1"
CHANNEL_PORT = 5000

WINDOW_SIZE = 4
SEQ_MOD = 2 * WINDOW_SIZE

TOTAL_PACKETS = 20

TIMEOUT = 1.0


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


class Sender:

    def __init__(self):

        self.send_base = 0
        self.next_seq = 0

        # Logical sequence number -> packet information.
        self.outstanding = {}

        self.acknowledged = set()

        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

        self.send_lock = threading.Lock()

        self.stop_event = threading.Event()

        self.ack_server = None
        self.channel_socket = None

    def send_packet(self, absolute_seq, retransmission=False):

        seq = absolute_seq % SEQ_MOD

        packet = {
            "type": "DATA",
            "seq": seq,
            "payload": f"Packet {absolute_seq}"
        }

        with self.send_lock:
            send_message(self.channel_socket, packet)

        if retransmission:
            print(f"[Sender] RETRANSMITTED packet {seq}")

        else:
            print(f"[Sender] Sent packet {seq}")

    def handle_ack(self, ack):

        seq = ack["seq"]

        with self.condition:

            # Map circular sequence number to current logical position.
            distance = (
                seq - (self.send_base % SEQ_MOD)
            ) % SEQ_MOD

            absolute_seq = self.send_base + distance

            if (
                absolute_seq < self.next_seq
                and absolute_seq in self.outstanding
            ):

                self.outstanding.pop(absolute_seq)

                self.acknowledged.add(absolute_seq)

                print(f"[Sender] Received ACK {seq}")

                # Slide sender window over consecutive ACKed packets.
                while self.send_base in self.acknowledged:

                    self.acknowledged.remove(self.send_base)

                    self.send_base += 1

                print(
                    f"[Sender] Window base = {self.send_base}"
                )

                self.condition.notify_all()

    def ack_listener(self):

        self.ack_server = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        self.ack_server.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        self.ack_server.bind(
            (SENDER_HOST, ACK_PORT)
        )

        self.ack_server.listen(1)

        print(f"[Sender] Listening for ACKs on port {ACK_PORT}")

        ack_socket, address = self.ack_server.accept()

        print(f"[Sender] Receiver connected: {address}")

        for ack in receive_messages(ack_socket):

            if self.stop_event.is_set():
                break

            if ack["type"] == "ACK":
                self.handle_ack(ack)

        ack_socket.close()

    def timer_monitor(self):

        while not self.stop_event.is_set():

            current_time = time.time()

            expired = []

            with self.condition:

                for absolute_seq, info in self.outstanding.items():

                    if current_time >= info["deadline"]:

                        expired.append(absolute_seq)

                        # Reset individual packet timer.
                        info["deadline"] = current_time + TIMEOUT

            # Retransmit expired packets outside the lock.
            for absolute_seq in expired:

                if self.stop_event.is_set():
                    break

                print(
                    f"[Sender] Timeout for packet "
                    f"{absolute_seq % SEQ_MOD}"
                )

                self.send_packet(
                    absolute_seq,
                    retransmission=True
                )

            time.sleep(0.05)

    def run(self):

        # Start ACK listener.
        ack_thread = threading.Thread(
            target=self.ack_listener
        )

        ack_thread.start()

        # Allow ACK server to initialize.
        time.sleep(0.2)

        # Connect to unreliable channel.
        while True:

            try:
                self.channel_socket = socket.socket(
                    socket.AF_INET,
                    socket.SOCK_STREAM
                )

                self.channel_socket.connect(
                    (CHANNEL_HOST, CHANNEL_PORT)
                )

                break

            except ConnectionRefusedError:
                self.channel_socket.close()
                time.sleep(0.2)

        print("[Sender] Connected to channel")

        # Start timer monitoring thread.
        timer_thread = threading.Thread(
            target=self.timer_monitor
        )

        timer_thread.start()

        # Send packets while respecting sender window.
        while self.send_base < TOTAL_PACKETS:

            packets_to_send = []

            with self.condition:

                while (
                    self.next_seq < TOTAL_PACKETS
                    and self.next_seq < self.send_base + WINDOW_SIZE
                ):

                    absolute_seq = self.next_seq

                    self.outstanding[absolute_seq] = {
                        "deadline": time.time() + TIMEOUT
                    }

                    packets_to_send.append(absolute_seq)

                    self.next_seq += 1

            for absolute_seq in packets_to_send:

                self.send_packet(absolute_seq)

            with self.condition:

                if self.send_base < TOTAL_PACKETS:
                    self.condition.wait(timeout=0.1)

        print("\n[Sender] All packets acknowledged!")

        # Stop timer monitoring.
        self.stop_event.set()

        timer_thread.join()

        # Inform receiver that transmission is complete.
        send_message(self.channel_socket, {
            "type": "FIN"
        })

        print("[Sender] Sent termination message")

        self.channel_socket.close()

        # Allow ACK listener to finish.
        ack_thread.join(timeout=2)

        self.ack_server.close()

        print("[Sender] Transfer completed successfully.")


if __name__ == "__main__":
    sender = Sender()
    sender.run()
