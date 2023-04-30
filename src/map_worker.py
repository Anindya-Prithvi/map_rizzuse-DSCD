import json
import os
import random
import secrets
from concurrent import futures
from threading import Lock

import grpc

import messages_pb2
import messages_pb2_grpc


class MapProcessInput(messages_pb2_grpc.MapProcessInputServicer):
    def __init__(self, mapper):
        self.mapper = mapper
        self.scripts = __import__("mapreduce")

    def Receive(self, request, context):
        """This function is called by the master node to send files"""
        if request.value == "<EOP>":
            self.mapper.ready_to_reduce = True
            # block everything until reducer finishes

            # close all files
            for file_handle in self.mapper.file_handles:
                # if lock acquired == n_reduce, then all files can be closed

                while True:
                    # if not locked then close file
                    __import__("time").sleep(0.1)
                    if not file_handle[0].locked():
                        file_handle[1].close()
                        break
        else:
            self.mapper.parse_and_map(self.mapper, request.value)

        return messages_pb2.Success(value="SUCCESS")

    def Info(self, request, context):
        """This function is called by the master node to send info"""
        # self.n_reduce = n_reduce
        # self.intermediate_storage = intermediate_storage
        # self.parse_and_map = parse_and_map -- derived from type
        # self.partition = partition
        # self.map = map

        kv = json.loads(request.value)

        self.mapper.n_reduce = kv["n_reduce"]
        self.mapper.intermediate_storage = kv["intermediate_storage"]
        os.mkdir(kv["intermediate_storage"] + "/" + self.mapper.intermediate_dir)

        curr = getattr(self.scripts, kv["type"])
        self.mapper.parse_and_map = curr.parse_and_map
        self.mapper.partition = curr.partition
        self.mapper.map = curr.map

        self.mapper.startlock.release()
        return messages_pb2.Success(value="SUCCESS")


class Mapper:
    def __init__(self):
        # create directory to store intermediate files
        self.intermediate_dir = f"map_{secrets.token_urlsafe(8)}"
        self.ready_to_reduce = False
        self.startlock = Lock()
        self.startlock.acquire()
        self.saver = {}
        PORT = str(random.randint(62000, 65535))

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
        self.server = server
        messages_pb2_grpc.add_MapProcessInputServicer_to_server(
            MapProcessInput(self), server
        )
        server.add_insecure_port(f"[::1]:{PORT}")  # no TLS moment
        server.start()

        # send message to master about liveliness
        # master will send other info later
        with grpc.insecure_channel("[::1]:6969") as channel:
            stub = messages_pb2_grpc.MasterRegistryStub(channel)
            response = stub.Receive(
                messages_pb2.InputMessage(value=f"[::1]:{PORT}", key="map")
            )
            assert response.value == "SUCCESS", "Master did not respond with success"

        self.startlock.acquire()
        if self.startlock.locked():
            self.startlock.release()
        # open n_reduce files with names 0, 1, 2, ..., n_reduce - 1 in the mapper's dir
        # write the key-value pairs to the corresponding file

        self.file_handles = []
        for i in range(self.n_reduce):
            self.file_handles.append(
                [
                    Lock(),
                    open(
                        f"{self.intermediate_storage}/{self.intermediate_dir}/{i}", "a"
                    ),
                ]
            )

        server.wait_for_termination()
