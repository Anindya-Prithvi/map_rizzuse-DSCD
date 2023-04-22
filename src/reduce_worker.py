import os
import secrets
from concurrent import futures

import grpc

import messages_pb2
import messages_pb2_grpc


class ReduceProcessInput(messages_pb2_grpc.ReduceProcessInputServicer):
    def Receive(self, request, context):
        # print("received")
        return messages_pb2.Success(value="SUCCESS")


class Reducer:
    def __init__(self, PORT, IP):
        # create directory to store intermediate files
        self.intermediate_dir = f"reduce_{secrets.token_urlsafe(8)}"

        if not os.path.exists("../reduce_intermediate"):
            os.mkdir("../reduce_intermediate")

        os.mkdir("../reduce_intermediate/" + self.intermediate_dir)

        port = str(PORT)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
        messages_pb2_grpc.add_ReduceProcessInputServicer_to_server(
            ReduceProcessInput(), server
        )
        server.add_insecure_port(IP + ":" + port)  # no TLS moment
        server.start()
        # logger.debug(f"{self.intermediate_dir} started on {IP}:{port}")
        server.wait_for_termination()

    # will only be called when IF received from all mappers
    def reduce(self):
        with open(self.input_file, "r") as f:
            for line in f:
                self.Reduce_line(line)
