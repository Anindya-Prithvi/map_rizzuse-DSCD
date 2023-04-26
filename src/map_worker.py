import os
import secrets
from concurrent import futures
from threading import Lock

import grpc

import messages_pb2
import messages_pb2_grpc


class MapProcessInput(messages_pb2_grpc.MapProcessInputServicer):
    def __init__(self, mapper):
        self.mapper = mapper

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


class Mapper:
    def __init__(
        self, PORT, IP, n_reduce, intermediate_storage, parse_and_map, partition, map
    ):
        # create directory to store intermediate files
        self.intermediate_dir = f"map_{secrets.token_urlsafe(8)}"
        self.ready_to_reduce = False
        self.n_reduce = n_reduce
        self.intermediate_storage = intermediate_storage
        self.parse_and_map = parse_and_map
        self.partition = partition
        self.map = map

        os.mkdir(path=intermediate_storage + "/" + self.intermediate_dir)

        port = str(PORT)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
        self.server = server
        messages_pb2_grpc.add_MapProcessInputServicer_to_server(
            MapProcessInput(self), server
        )
        server.add_insecure_port(IP + ":" + port)  # no TLS moment

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

        server.start()
        server.wait_for_termination()
