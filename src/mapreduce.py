import os,sys

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


class II:
    def parse_and_map(self, file):
        """Prepares input for map function"""
        with open(file, "r") as f:
            input_data = f.read()
            lines = input_data.split("\n")
            for line in lines:
                self.partition(self, self.map(self, file, line.strip()))

    def map(self, key, value):
        line_words = value.split()
        l_k_v = [(word.lower(), key) for word in line_words]  # wc case insensitive
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

    def reduce(self):
        """function to reduce the values that belong to the same key."""
        with open(f"{self.output_dir}/Output{self.node_name}.txt", "w") as f:
            for key in self.hashbucket:
                f.write(f"{key} {self.hashbucket[key]}\n")

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
                if key not in self.hashbucket:
                    self.hashbucket[key] = set()
                self.hashbucket[key].add(value)  

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