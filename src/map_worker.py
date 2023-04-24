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
            self.mapper.parse_and_map(request.value)

        return messages_pb2.Success(value="SUCCESS")


class Mapper:
    def __init__(self, PORT, IP, n_reduce):
        # create directory to store intermediate files
        self.intermediate_dir = f"map_{secrets.token_urlsafe(8)}"
        self.ready_to_reduce = False
        self.n_reduce = n_reduce

        if not os.path.exists("../map_intermediate"):
            os.mkdir("../map_intermediate")

        os.mkdir("../map_intermediate/" + self.intermediate_dir)

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
                [Lock(), open(f"../map_intermediate/{self.intermediate_dir}/{i}", "a")]
            )

        server.start()
        server.wait_for_termination()

    def parse_and_map(self, file):
        """Prepares input for map function"""
        with open(file, "r") as f:
            input_data = f.read()
            lines = input_data.split("\n")
            for i, line in enumerate(lines):
                self.partition(self.map(i, line))

    def map(self, key, value):
        """Map function to each input split to generate
        intermediate key-value pairs. The Map function takes a
        key-value pair as input and produces a set of
        intermediate key-value pairs as output. The output of
        each Map function should be written to a file in the
        mapper's directory on the local file system. Note
        that each mapper will be run as a different process.
        """

        line_words = value.split()
        l_k_v = [(word, 1) for word in line_words]
        return l_k_v

    def partition(self, l_k_v):
        """write a function that takes the list of key-value
        pairs generated by the Map function and partitions them
        into a set of smaller partitions. The partitioning
        function should ensure that all key-value pairs
        with the same key are sent to the same partition.
        Each partition is then picked up by a specific reducer
        during shuffling and sorting.
        """

        # will generate n_reduce intermediate files

        for key, value in l_k_v:
            # hash the key and mod it with n_reduce
            # write the key-value pair to the corresponding file
            self.file_handles[hash(key) % self.n_reduce][0].acquire()
            self.file_handles[hash(key) % self.n_reduce][1].write(f"{key} {value}\n")
            self.file_handles[hash(key) % self.n_reduce][0].release()
