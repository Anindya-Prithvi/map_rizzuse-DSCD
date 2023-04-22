"""This is the file for master node"""


import argparse
import math
import multiprocessing
import os
import shutil
from concurrent import futures
from functools import reduce

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

        logger.debug(f"input splits: {self.partitions}")

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
                                messages_pb2.InputMessage(
                                    key=str(key) + partition, value=line
                                )
                            )
                            try:
                                assert response.value == "SUCCESS"
                            except AssertionError:
                                return False

            # send an EOF message too
            with grpc.insecure_channel(mapper["addr"]) as channel:
                logger.debug(f"Sending <EOF> to mapper {mapper['addr']}")
                stub = messages_pb2_grpc.MapProcessInputStub(channel)
                response = stub.Receive(
                    messages_pb2.InputMessage(key="<EOF>", value="<EOF>")
                )
                try:
                    assert response.value == "SUCCESS"
                except AssertionError:
                    return False

            return True

        tpool = futures.ThreadPoolExecutor(max_workers=self.n_map)
        tpool_map = tpool.map(send_shard, self.mappers, self.partitions)

        if reduce(lambda x, y: x and y, tpool_map):
            logger.debug("Job finished successfully.")
        else:
            logger.debug("Some nodes seem to have failed.")

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

        # check all live mapper processes
        for mapper in self.mappers:
            if not mapper["process"].is_alive():
                raise Exception(f"Mapper process {mapper} died.")

        # check all live reducer processes
        for reducer in self.reducers:
            if not reducer["process"].is_alive():
                raise Exception(f"Reducer process {mapper} died.")

        logger.debug("All child nodes initialized.")

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
    logger.info("Waiting for nodes to initialize and bind...")
    __import__("time").sleep(1.6)
    try:
        master.run()
    except KeyboardInterrupt:
        logger.warning("Keyboard Interrupt. Terminating nodes.")
    except Exception as e:
        logger.error(e)

    master.destroy_nodes()
    shutil.rmtree("../reduce_intermediate")
    shutil.rmtree("../map_intermediate")
