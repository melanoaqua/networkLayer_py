from mix_core import MixCore
from core import SphinxPacker, generate_random_string
from sphinxmix.SphinxClient import Relay_flag, Dest_flag

class ProviderCore(MixCore):
    def __init__(self, params, name, port, host, privk, pubk):
        MixCore.__init__(self, params, name, port, host, privk, pubk)

        self.packer = SphinxPacker(params)
        self.name = name
        self.port = port
        self.host = host
        self.privk = privk
        self.pubk = pubk

    def create_dummy_message(self, receiver):
        path = [receiver]
        dummy_message = 'HD' + generate_random_string(self.config.NOISE_LENGTH)
        header, body = self.packer.make_sphinx_packet(
            receiver=receiver, path=path, message=dummy_message)
        return header, body

    def process_packet(self, packet):
        tag, routing, new_header, new_body = self.packer.decrypt_sphinx_packet(packet, self.privk)
        routing_flag, meta_info = routing[0], routing[1:]
        if routing_flag == Relay_flag:
            next_addr, drop_flag, type_flag, delay, next_name = meta_info[0]
            if drop_flag:
                return "DROP", []
            else:
                return "ROUT", [delay, new_header, new_body, next_addr, next_name]
        elif routing_flag == Dest_flag:
            dest, message = self.packer.handle_received_forward(new_body)
            if dest == [self.host, self.port, self.name]:
                if message.startswith('HT'):
                    return "LOOP", [message]
            else:
                return "ERROR", []
