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


class NJ:
	def __init__(self, input_files):
		self.input_files = input_files

	def mapper(self, input_file):
		rows = []
		with open(input_file, "r") as f:
			next(f) 
			for line in f:
				key, value = line.strip().split(",")
				rows.append((key, input_file + "," + value))
		return rows

	def reducer(self, key, values):
		final_ans = []
		table1 = []
		table2 = []

		for value in values:
			table, row = value.split(', ')
			if(table=="input1.txt" or table=="input3.txt"):
				table1.append(row)
			else:
				table2.append(row)

		for t in table1:
			for u in table2:
				temp = [key,t,u]
				final_ans.append(temp)

		return final_ans

	def run(self):
		mapped_rows = []
		for input_file in self.input_files:
			mapped_rows.extend(self.mapper(input_file))

		grouped_rows = {}
		for row in mapped_rows:
			key, value = row
			if key in grouped_rows:
				grouped_rows[key].append(value)
			else:
				grouped_rows[key] = [value]

		with open('output.txt', 'w') as output_file:
			for key, values in grouped_rows.items():
				output = self.reducer(key, values)
				if output:
					for result in output:
						output_file.write(", ".join(result) + "\n")