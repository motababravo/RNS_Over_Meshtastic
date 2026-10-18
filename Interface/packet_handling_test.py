from Meshtastic_Interface import PacketHandler
import struct

if __name__ == '__main__':
    handler = PacketHandler(b'hello_world'*1, 0)
    receive = PacketHandler()
    while not handler.is_done():
        packet = handler.get_next()
        result = receive.process_packet(packet)
    print(result == b'hello_world'*1)
