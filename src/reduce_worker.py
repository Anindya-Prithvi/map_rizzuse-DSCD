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
        self.reducer.parse_map_loc(self.reducer, request.value)

        return messages_pb2.Success(value="SUCCESS")


class Reducer:
    def __init__(self, PORT, IP, n_map, output_dir, parse_map_loc, reduce, shufflesort):
        # create directory to store intermediate files
        self.node_name = None
        self.hashbucket = {}
        self.output_dir = output_dir
        self.parse_map_loc = parse_map_loc
        self.reduce = reduce
        self.shufflesort = shufflesort

        port = str(PORT)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
        messages_pb2_grpc.add_ReduceProcessInputServicer_to_server(
            ReduceProcessInput(self), server
        )
        server.add_insecure_port(IP + ":" + port)  # no TLS moment
        server.start()
        # logger.debug(f"{self.intermediate_dir} started on {IP}:{port}")
        server.wait_for_termination()
