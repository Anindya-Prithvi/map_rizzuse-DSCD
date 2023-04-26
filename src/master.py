"""This is the file for master node"""


import argparse
import math
import multiprocessing
import os
from concurrent import futures
from functools import reduce

import grpc
from loguru import logger

import messages_pb2
import messages_pb2_grpc
from map_worker import Mapper
from reduce_worker import Reducer


class WC:
    def parse_and_map(self, file):
        """Prepares input for map function"""
        with open(file, "r") as f:
            input_data = f.read()
            lines = input_data.split("\n")
            for i, line in enumerate(lines):
                self.partition(self, self.map(self, i, line.strip()))

    def map(self, key, value):
        line_words = value.split()
        l_k_v = [(word.lower(), 1) for word in line_words]  # wc case insensitive
        return l_k_v

    def partition(self, l_k_v):
        from hashlib import md5

        hash = md5
        # will generate n_reduce intermediate files

        for key, value in l_k_v:
            key_int = int(hash(key.encode()).hexdigest(), 16)
            # hash the key and mod it with n_reduce
            # write the key-value pair to the corresponding file
            self.file_handles[key_int % self.n_reduce][0].acquire()
            self.file_handles[key_int % self.n_reduce][1].write(f"{key} {value}\n")
            self.file_handles[key_int % self.n_reduce][0].release()

    def parse_map_loc(self, map_loc):
        """This function will read all assigned intermediate files of the mapper"""
        for file in os.listdir(map_loc):
            self.shufflesort(self, os.path.join(map_loc, file, self.node_name))
        self.reduce(self)

    # will only be called when IF received from all mappers
    def reduce(self):
        """function to reduce the values that belong to the same key."""
        with open(f"{self.output_dir}/Output{self.node_name}.txt", "w") as f:
            for key in self.hashbucket:
                f.write(f"{key} {sum(self.hashbucket[key])}\n")

    def shufflesort(self, file):
        """function to sort the intermediate key-value pairs by key and
        group the values that belong to the same key.
        """
        # check if file exists [no work for reducer]
        if not os.path.isfile(file):
            return
        with open(file, "r") as f:
            for line in f:
                key, value = line.strip().split(" ")
                value = int(value)
                if key not in self.hashbucket:
                    self.hashbucket[key] = []
                self.hashbucket[key].append(value)  # mostly 1 since no local reduce


class Master:
    """Master node class"""

    def __init__(
        self,
        input_data,
        output_data,
        n_map,
        n_reduce,
        intermediate="../map_intermediate",
    ):
        self.input_data = input_data
        self.output_data = output_data
        self.n_map = n_map
        self.n_reduce = n_reduce
        self.mappers = []
        self.reducers = []
        self.intermediate = intermediate
        logger.debug("Master node initialized. Starting child nodes.")
        self.initialize_nodes()
        logger.debug("Initializing complete.")

    def run(self):
        """Whenever run is called, the master node should will submit
        the input data to the mappers and wait for job completion."""
        logger.debug("Sending file locations to mappers.")

        # this will create partitions for each mapper
        self.input_split()

        logger.debug(f"input splits: {self.partitions}")

        # Master should not read the actual content of the files. Master should just
        # pass the file location to the mappers. Mappers should read the files from
        # the file location.
        def send_shard(mapper, partitions):
            # partitions are basically files

            for partition in partitions:
                with grpc.insecure_channel(mapper["addr"]) as channel:
                    stub = messages_pb2_grpc.MapProcessInputStub(channel)
                    response = stub.Receive(
                        messages_pb2.InputMessage(
                            value=os.path.join(self.input_data, partition)
                        )
                    )
                    try:
                        assert response.value == "SUCCESS"
                    except AssertionError:
                        return False

            # send an EOP (end of partitions) message too
            with grpc.insecure_channel(mapper["addr"]) as channel:
                logger.debug(f"Sending <EOP> to mapper {mapper['addr']}")
                stub = messages_pb2_grpc.MapProcessInputStub(channel)
                response = stub.Receive(
                    messages_pb2.InputMessage(key="<EOP>", value="<EOP>")
                )
                try:
                    assert response.value == "SUCCESS"
                except AssertionError:
                    return False

            return True

        tpool = futures.ThreadPoolExecutor(max_workers=self.n_map)
        tpool_map = tpool.map(send_shard, self.mappers, self.partitions)

        if reduce(lambda x, y: x and y, tpool_map):
            logger.debug("Map phase finished successfully.")
        else:
            logger.debug("Some nodes seem to have failed.")

        tpool.shutdown()

        # time for reduce phase
        logger.debug("Sending intermediate file locations to reducers.")

        def send_IF(reducer, node_num):
            with grpc.insecure_channel(reducer["addr"]) as channel:
                stub = messages_pb2_grpc.ReduceProcessInputStub(channel)
                response = stub.Receive(
                    messages_pb2.InputMessage(
                        key=str(node_num), value=self.intermediate
                    )
                )
                try:
                    assert response.value == "SUCCESS"
                except AssertionError:
                    return False

                return True

        tpool = futures.ThreadPoolExecutor(max_workers=self.n_reduce)
        tpool_map = tpool.map(send_IF, self.reducers, range(self.n_reduce))

        if reduce(lambda x, y: x and y, tpool_map):
            logger.debug("Reduce phase finished successfully.")
        else:
            logger.debug("Some nodes seem to have failed.")

        tpool.shutdown()

        logger.debug("Job finished.")

    def initialize_nodes(self):
        """On a production scale we can ask a central(registry) server
        to create the mappers and reducers for us. Not here tho."""
        """Initialize and register the mappers"""

        if not os.path.exists(self.intermediate):
            os.mkdir(self.intermediate)

        if not os.path.exists(self.output_data):
            os.mkdir(self.output_data)

        for i in range(self.n_map):
            p = multiprocessing.Process(
                target=Mapper,
                kwargs={
                    "PORT": 21337 + i,
                    "IP": "[::1]",
                    "n_reduce": self.n_reduce,
                    "intermediate_storage": self.intermediate,
                    "partition": WC.partition,
                    "parse_and_map": WC.parse_and_map,
                    "map": WC.map,
                },
            )
            p.start()
            self.mappers.append({"process": p, "addr": f"[::1]:{21337 + i}"})

        """Initialize and register the reducers"""
        for i in range(self.n_reduce):
            p = multiprocessing.Process(
                target=Reducer,
                kwargs={
                    "PORT": 31337 + i,
                    "IP": "[::1]",
                    "n_map": self.n_map,
                    "output_dir": self.output_data,
                    "reduce": WC.reduce,
                    "parse_map_loc": WC.parse_map_loc,
                    "shufflesort": WC.shufflesort,
                },
            )
            p.start()
            self.reducers.append({"process": p, "addr": f"[::1]:{31337 + i}"})

        logger.info("Waiting for nodes to initialize and bind...")
        __import__("time").sleep(1.6)
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
        input_files = os.listdir(self.input_data)  # no filtering
        files_per_mapper = math.ceil(len(os.listdir(self.input_data)) / self.n_map)
        self.partitions = [
            input_files[i : i + files_per_mapper]
            for i in range(0, len(input_files), files_per_mapper)
        ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Input data directory", required=True)
    parser.add_argument("--output", help="Output data directory", required=True)
    parser.add_argument(
        "--intermediate",
        help="Intermediate map data directory",
        default="../map_intermediate",
    )

    # either have --config or have both --n_map and --n_reduce
    parser.add_argument("--config", help="Config file")
    parser.add_argument("--n_map", help="Number of mappers", type=int)
    parser.add_argument("--n_reduce", help="Number of reducers", type=int)
    args = parser.parse_args()

    if args.config:
        with open(args.config, "r") as f:
            config = f.read().strip().split("\n")
            # make dictionary of options like "key = value"
            config = {(_i := i.split("="))[0].strip(): _i[1].strip() for i in config}
            args.n_map = int(config["Mappers"])
            args.n_reduce = int(config["Reducers"])
    else:
        assert (
            args.n_map and args.n_reduce
        ), "Either provide a config file or provide both --n_map and --n_reduce"

    logger.debug(f"mappers: {args.n_map}, reducers: {args.n_reduce}")

    master = Master(
        args.input, args.output, args.n_map, args.n_reduce, args.intermediate
    )

    try:
        master.run()
    except KeyboardInterrupt:
        logger.warning("Keyboard Interrupt. Terminating nodes.")
    except Exception as e:
        logger.error(e)

    master.destroy_nodes()
    # import shutil
    # shutil.rmtree("../reduce_intermediate")
    # shutil.rmtree("../map_intermediate")
