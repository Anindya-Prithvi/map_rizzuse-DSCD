import json
import random
from concurrent import futures
from threading import Lock

import grpc

import messages_pb2
import messages_pb2_grpc


class ReduceProcessInput(messages_pb2_grpc.ReduceProcessInputServicer):
    def __init__(self, reducer):
        self.reducer = reducer
        self.scripts = __import__("mapreduce")

    def Receive(self, request, context):
        """This function is called by the master node to send files"""
        self.reducer.node_name = request.key
        self.reducer.parse_map_loc(self.reducer, request.value)

        return messages_pb2.Success(value="SUCCESS")

    def Info(self, request, context):
        """This function is called by the master node to send info"""
        kv = json.loads(request.value)

        self.reducer.n_map = kv["n_map"]
        self.reducer.output_dir = kv["output_dir"]
        curr = getattr(self.scripts, kv["type"])
        self.reducer.parse_map_loc = curr.parse_map_loc
        self.reducer.reduce = curr.reduce
        self.reducer.shufflesort = curr.shufflesort

        self.reducer.startlock.release()

        return messages_pb2.Success(value="SUCCESS")


class Reducer:
    def __init__(self):
        # create directory to store intermediate files
        self.node_name = None
        self.hashbucket = {}
        self.startlock = Lock()
        self.startlock.acquire()

        PORT = str(random.randint(62000, 65535))
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
        messages_pb2_grpc.add_ReduceProcessInputServicer_to_server(
            ReduceProcessInput(self), server
        )
        server.add_insecure_port(f"[::1]:{PORT}")  # no TLS moment

        # get port of server
        server.start()

        with grpc.insecure_channel("[::1]:6969") as channel:
            stub = messages_pb2_grpc.MasterRegistryStub(channel)
            response = stub.Receive(
                messages_pb2.InputMessage(value=f"[::1]:{PORT}", key="reduce")
            )
            assert response.value == "SUCCESS", "Master did not respond with success"

        # logger.debug(f"{self.intermediate_dir} started on {IP}:{port}")
        server.wait_for_termination()
