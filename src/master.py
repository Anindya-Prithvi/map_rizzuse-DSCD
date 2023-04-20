"""This is the file for master node"""


import argparse
import math
import multiprocessing
import os
import shutil
from concurrent import futures
from time import sleep

import grpc
from loguru import logger

import messages_pb2
import messages_pb2_grpc
from map_worker import Mapper
from reduce_worker import Reducer


class Master:
    """Master node class"""

    def __init__(self, input_data, output_data, n_map, n_reduce):
        self.input_data = input_data
        self.output_data = output_data
        self.n_map = n_map
        self.n_reduce = n_reduce
        self.mappers = []
        self.reducers = []
        logger.debug("Master node initialized. Starting child nodes.")
        self.initialize_nodes()
        logger.debug("Initializing complete.")

    def run(self):
        """Whenever run is called, the master node should will submit
        the input data to the mappers and wait for job completion."""
        logger.debug("Starting Input Split.")

        # this will create partitions for each mapper
        self.input_split()

        print(self.partitions)

        def send_shard(mapper, partitions):
            # partitions are basically files
            # like hadoop, we shall key in line number, value will be readline

            for partition in partitions:
                with open(os.path.join(self.input_data, partition), "r") as f:
                    # Read the input data file
                    filecontent = f.read()
                    lines = filecontent.split("\n")

                    for key, line in enumerate(lines):
                        # Send the key, value pair to the mapper

                        with grpc.insecure_channel(mapper["addr"]) as channel:
                            stub = messages_pb2_grpc.MapProcessInputStub(channel)
                            response = stub.Receive(
                                messages_pb2.InputMessage(key=str(key), value=line)
                            )
                            assert response.value == "SUCCESS"

        tpool = futures.ThreadPoolExecutor(max_workers=self.n_map)
        tpool.map(send_shard, self.mappers, self.partitions)

        sleep(100000)
        tpool.shutdown()

    def initialize_nodes(self):
        """On a production scale we can ask a central(registry) server
        to create the mappers and reducers for us. Not here tho."""
        """Initialize and register the mappers"""
        for i in range(self.n_map):
            p = multiprocessing.Process(
                target=Mapper, kwargs={"PORT": 21337 + i, "IP": "[::1]"}
            )
            p.start()
            self.mappers.append({"process": p, "addr": f"[::1]:{21337 + i}"})

        """Initialize and register the reducers"""
        for i in range(self.n_reduce):
            p = multiprocessing.Process(
                target=Reducer, kwargs={"PORT": 31337 + i, "IP": "[::1]"}
            )
            p.start()
            self.reducers.append({"process": p, "addr": f"[::1]:{31337 + i}"})

    def destroy_nodes(self):
        """Destroy the mappers"""
        for mapper in self.mappers:
            mapper["process"].terminate()

        """Destroy the reducers"""
        for reducer in self.reducers:
            reducer["process"].terminate()

    def input_split(self):
        """For simplicity, you may assume that the input data
        consists of multiple data files and each file is
        processed by a separate mapper.
        """
        input_files = os.listdir(self.input_data)
        files_per_mapper = math.ceil(len(os.listdir(self.input_data)) / self.n_map)
        self.partitions = [
            input_files[i : i + files_per_mapper]
            for i in range(0, len(input_files), files_per_mapper)
        ]

    # key -> document name, value -> document content  || figure out how to go about map function parameters
    def map(self, key, value, input_files):
        """Map function to each input split to generate
        intermediate key-value pairs. The Map function takes a
        key-value pair as input and produces a set of
        intermediate key-value pairs as output. The output of
        each Map function should be written to a file in the
        mapper's directory on the local file system. Note
        that each mapper will be run as a different process.
        """
        for input_file in input_files:
            with open(os.path.join(self.input_data, input_file), "r") as f:
                # Read the input data file
                input_data = f.read()
                lines = input_data.split("\n")

                words = []
                for line in lines:
                    line_words = line.split()
                    words.extend(line_words)

                intermediate_key_values = set()
                # TODO: Check whether the frequency will be updated in here
                for word in words:
                    intermediate_key_values.add((word, "1"))

                # Write the intermediate key-value pairs to a file in the mapper's directory
                output_file = os.path.join(self.output_data, input_file)
                with open(output_file, "w") as f:
                    for intermediate_key, intermediate_value in intermediate_key_values:
                        f.write("{}\t{}\n".format(intermediate_key, intermediate_value))

    def partition(self):
        """write a function that takes the list of key-value
        pairs generated by the Map function and partitions them
        into a set of smaller partitions. The partitioning
        function should ensure that all key-value pairs
        with the same key are sent to the same partition.
        Each partition is then picked up by a specific reducer
        during shuffling and sorting.
        """
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Input data directory", required=True)
    parser.add_argument("--output", help="Output data directory", required=True)
    parser.add_argument("--n_map", help="Number of mappers", required=True, type=int)
    parser.add_argument(
        "--n_reduce", help="Number of reducers", required=True, type=int
    )
    args = parser.parse_args()

    master = Master(args.input, args.output, args.n_map, args.n_reduce)
    logger.info("Waiting for nodes to initialize...")
    __import__("time").sleep(3)
    try:
        master.run()
    except KeyboardInterrupt:
        logger.warning("Keyboard Interrupt. Terminating nodes.")
    except Exception as e:
        logger.error(e)

    master.destroy_nodes()
    shutil.rmtree("../reduce_intermediate")
    shutil.rmtree("../map_intermediate")
