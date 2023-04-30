"""This is the file for master node"""


import argparse
import json
import math
import multiprocessing
import os
from concurrent import futures
from functools import reduce
from threading import Lock

import grpc
from loguru import logger

import messages_pb2
import messages_pb2_grpc
from map_worker import Mapper
from reduce_worker import Reducer


class MasterRegistry(messages_pb2_grpc.MasterRegistryServicer):
    def __init__(self, master):
        self.startlock = master.startlock
        self.master = master

    def Receive(self, request, context):
        """This function is called workers to notify their location.
        key will be either "map" or "reduce"
        value will be the IP:PORT of the worker
        """
        if request.key == "map":
            self.master.mappers.append(request.value)
            # also send task of mapper
            with grpc.insecure_channel(request.value) as channel:
                stub = messages_pb2_grpc.MapProcessInputStub(channel)
                response = stub.Info(
                    messages_pb2.InputMessage(
                        value=json.dumps(
                            {
                                "n_reduce": self.master.n_reduce,
                                "intermediate_storage": self.master.intermediate,
                                "type": self.master.objective.__name__,
                            }
                        )
                    )
                )
                assert response.value == "SUCCESS"

        elif request.key == "reduce":
            self.master.reducers.append(request.value)
            # also send task of reducer
            with grpc.insecure_channel(request.value) as channel:
                stub = messages_pb2_grpc.ReduceProcessInputStub(channel)
                response = stub.Info(
                    messages_pb2.InputMessage(
                        value=json.dumps(
                            {
                                "n_map": self.master.n_map,
                                "output_dir": self.master.output_data,
                                "type": self.master.objective.__name__,
                            }
                        )
                    )
                )
                assert response.value == "SUCCESS"

        if (
            len(self.master.mappers) == self.master.n_map
            and len(self.master.reducers) == self.master.n_reduce
        ):
            if self.startlock.locked():
                self.startlock.release()
                # may be released twice: non critical race condition
        return messages_pb2.Success(value="SUCCESS")


class Master:
    """Master node class"""

    def __init__(
        self,
        input_data,
        output_data,
        n_map,
        n_reduce,
        objective,
        intermediate="../map_intermediate",
    ):
        self.scripts = __import__("mapreduce")
        self.input_data = input_data
        self.output_data = output_data
        self.n_map = n_map
        self.n_reduce = n_reduce
        self.processes = []  # anonymous
        self.mappers = []
        self.reducers = []
        self.objective = getattr(self.scripts, objective)
        self.intermediate = intermediate
        self.startlock = Lock()
        self.startlock.acquire()

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=69))
        self.server = server
        self.server = server
        messages_pb2_grpc.add_MasterRegistryServicer_to_server(
            MasterRegistry(self), server
        )
        server.add_insecure_port("[::]:6969")  # binding to 0.0.0.0
        server.start()
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
                with grpc.insecure_channel(mapper) as channel:
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
            with grpc.insecure_channel(mapper) as channel:
                logger.debug(f"Sending <EOP> to mapper {mapper}")
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
            with grpc.insecure_channel(reducer) as channel:
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

        if not os.path.exists(self.intermediate):
            os.mkdir(self.intermediate)

        if not os.path.exists(self.output_data):
            os.mkdir(self.output_data)

        """Initialize the mappers"""
        for i in range(self.n_map):
            p = multiprocessing.Process(
                target=Mapper,
            )
            p.start()
            self.processes.append(p)

        """Initialize the reducers"""
        for i in range(self.n_reduce):
            p = multiprocessing.Process(
                target=Reducer,
            )
            p.start()
            self.processes.append(p)

        logger.info("Waiting for nodes to initialize and bind...")
        self.startlock.acquire()
        # check all live processes
        for p in self.processes:
            if not p.is_alive():
                logger.error("Some nodes seem to have failed.")
                self.destroy_nodes()
                exit(1)

        # now we will wait for all mappers and reducers to notify master
        logger.debug("All child nodes initialized.")
        if self.startlock.locked():
            self.startlock.release()  # non critical

    def destroy_nodes(self):
        """Terminate all processes"""
        for process in self.processes:
            process.terminate()

    def input_split(self):
        """For simplicity, you may assume that the input data
        consists of multiple data files and each file is
        processed by a separate mapper.
        """
        if self.objective == "NJ":
            input_files = os.listdir(self.input_data)  # no filtering
            input_tables = []
            for file_name in input_files:
                input_number, table_name = file_name.split("_")
                input_found = False
                for i, input_list in enumerate(input_tables):
                    if input_list[0].startswith(input_number):
                        input_list.append(file_name)
                        input_found = True
                        break
                if not input_found:
                    input_tables.append([file_name])
            self.partitions = input_tables
        else:
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
        args.input, args.output, args.n_map, args.n_reduce, "WC", args.intermediate
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
