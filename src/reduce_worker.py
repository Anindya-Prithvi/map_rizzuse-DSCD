from concurrent import futures
import secrets
import grpc
import messages_pb2, messages_pb2_grpc
import os


class ReduceProcessInput(messages_pb2_grpc.ReduceProcessInputServicer):
    def __init__(self):
        super().__init__()

    # def GetServerList(self, request, context):
    #     self.logger.info(
    #         "SERVER LIST REQUEST FROM %s",
    #         context.peer(),
    #     )
    #     return self.registered

    def receive(self, request, context):
        print("received")
        return messages_pb2.Success(value="SUCCESS")

class Reducer:
    def __init__(self, PORT, IP):
        # create directory to store intermediate files
        self.intermediate_dir = f"reduce_{secrets.token_urlsafe(8)}"

        if not os.path.exists("../reduce_intermediate"):
            os.mkdir("../reduce_intermediate")

        os.mkdir("../reduce_intermediate/"+self.intermediate_dir)

        port = str(PORT)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
        messages_pb2_grpc.add_ReduceProcessInputServicer_to_server(ReduceProcessInput(), server)
        server.add_insecure_port(IP + ":" + port)  # no TLS moment
        server.start()
        print(f"{self.intermediate_dir} started on {IP}:{port}")
        while True:
            # do something compute intensive
            print("doing something")

    def reduce(self):
        with open(self.input_file, "r") as f:
            for line in f:
                self.Reduce_line(line)

