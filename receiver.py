import socket
import json
import time
RECEIVER_HOST = "127.0.0.1"
RECEIVER_PORT = 5001
SENDER_HOST = "127.0.0.1"
SENDER_ACK_PORT = 5002
WINDOW_SIZE = 4
SEQ_MOD = 2 * WINDOW_SIZE
def receive_messages(sock):
    buffer = b
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
    """Serialize a dictionary and send it as JSON."""
    data = json.dumps(message).encode() + b"\n"
    sock.sendall(data)
class Receiver:
    def __init__(self):
        self.rcv_base = 0
        self.buffer = {}
        self.server_socket = None
        self.ack_socket = None
    def process_packet(self, packet):
        if packet["type"] == "FIN":
            print("\n[Receiver] Transfer completed.")
            return False
        seq = packet["seq"]
        expected = self.rcv_base % SEQ_MOD
        distance = (seq - expected) % SEQ_MOD
        absolute_seq = self.rcv_base + distance
        if self.rcv_base <= absolute_seq < self.rcv_base + WINDOW_SIZE:
            if absolute_seq not in self.buffer:
                self.buffer[absolute_seq] = packet["payload"]
                print(
                    f"[Receiver] Received packet {seq} "
                    f"(position {absolute_seq})"
                )
            else:
                print(f"[Receiver] Duplicate packet {seq}")
            send_message(self.ack_socket, {
                "type": "ACK",
                "seq": seq
            })
            print(f"[Receiver] Sent ACK {seq}")
            while self.rcv_base in self.buffer:
                payload = self.buffer.pop(self.rcv_base)
                print(
                    f"[Receiver] Delivered packet "
                    f"{self.rcv_base}: {payload}"
                )
                self.rcv_base += 1
        else:
            behind = (expected - seq) % SEQ_MOD
            if 0 < behind <= WINDOW_SIZE:
                print(f"[Receiver] Old duplicate packet {seq}")
                send_message(self.ack_socket, {
                    "type": "ACK",
                    "seq": seq
                })
                print(f"[Receiver] Re-sent ACK {seq}")
            else:
                print(f"[Receiver] Rejected out-of-window packet {seq}")
        return True
    def run(self):
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
            (RECEIVER_HOST, RECEIVER_PORT)
        )
        self.server_socket.listen(1)
        print(f"[Receiver] Listening on port {RECEIVER_PORT}")
        data_socket, address = self.server_socket.accept()
        print(f"[Receiver] Channel connected: {address}")
        while True:
            try:
                self.ack_socket = socket.socket(
                    socket.AF_INET,
                    socket.SOCK_STREAM
                )
                self.ack_socket.connect(
                    (SENDER_HOST, SENDER_ACK_PORT)
                )
                break
            except ConnectionRefusedError:
                self.ack_socket.close()
                time.sleep(0.2)
        print("[Receiver] Connected to sender for ACKs")
        for packet in receive_messages(data_socket):
            if not self.process_packet(packet):
                break
        data_socket.close()
        self.ack_socket.close()
        self.server_socket.close()
        print("[Receiver] Shutdown complete.")
if __name__ == "__main__":
    receiver = Receiver()
    receiver.run()
