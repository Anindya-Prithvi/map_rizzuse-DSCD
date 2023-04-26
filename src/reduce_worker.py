import os
import secrets
from concurrent import futures

import grpc

import messages_pb2
import messages_pb2_grpc


class ReduceProcessInput(messages_pb2_grpc.ReduceProcessInputServicer):
    def __init__(self, reducer):
        self.reducer = reducer

    def Receive(self, request, context):
        """This function is called by the master node to send files"""
        self.reducer.node_name = request.key
        self.reducer.parse_map_loc(request.value)

        return messages_pb2.Success(value="SUCCESS")


class Reducer:
    def __init__(self, PORT, IP, n_map, output_dir):
        # create directory to store intermediate files
        self.intermediate_dir = f"reduce_{secrets.token_urlsafe(8)}"
        self.node_name = None

        if not os.path.exists(output_dir):
            os.mkdir(output_dir)

        os.mkdir(output_dir + "/" + self.intermediate_dir)

        port = str(PORT)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
        messages_pb2_grpc.add_ReduceProcessInputServicer_to_server(
            ReduceProcessInput(self), server
        )
        server.add_insecure_port(IP + ":" + port)  # no TLS moment
        server.start()
        # logger.debug(f"{self.intermediate_dir} started on {IP}:{port}")
        server.wait_for_termination()

    def parse_map_loc(self, map_loc):
        """This function will read all assigned intermediate files of the mapper"""
        for file in os.listdir(map_loc):
            self.reduce(os.path.join(map_loc, file, self.node_name))

    # will only be called when IF received from all mappers
    def reduce(self, file):
        # TODO: figure out how to handle maintaining the mappings in global
        key_values = {}

        # read file line by line and add to key_values
        with open(file, "r") as f:
            for line in f:
                print(f"[{self.node_name}] reducing {line}")
                key, value = line.strip().split("\t")
                if key in key_values:
                    key_values[key].append(int(value))
                else:
                    key_values[key] = [int(value)]

        # write to output file (reducing step)
        with open("OUTPUT_FILE_PATH", "w") as output_file:
            for key in sorted(key_values.keys()):
                total = sum(key_values[key])
                output_file.write(f"{key}\t{total}\n")
