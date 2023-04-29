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
    def parse_and_map(self, file):
        with open(file, "r") as f:
            lines = f.readlines()
        headers = [lines[0].strip().split(", ")]
        mapping_dict = {}
        mapping_dict[headers[0]] = []
        mapping_dict[headers[1]] = []
        for line in lines[1:]:
            header_val_1, header_val_2 = line.strip().split(", ")
            mapping_dict[headers[0]].append(list(header_val_1, file, header_val_2, headers[1]))
        for line in lines[1:]:
            header_val_1, header_val_2 = line.strip().split(", ")
            mapping_dict[headers[1]].append(list(header_val_2, file, header_val_1, headers[0]))
        self.partition(self, mapping_dict)

    def partition(self, mapping_dict):
        from hashlib import md5

        hash = md5
        # will generate n_reduce intermediate files

        for key in mapping_dict:
            key_int = int(hash(key.encode()).hexdigest(), 16)
            # hash the key and mod it with n_reduce
            # write the key-value pair to the corresponding file
            self.file_handles[key_int % self.n_reduce][0].acquire()
            self.file_handles[key_int % self.n_reduce][1].write(f"{key} {mapping_dict[key]}\n")
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
                f.write(f"{key} {self.hashbucket[key][0][3]} {self.hashbucket[key][1][3]}\n")
                if (len(self.hashbucket[key]) == 2):
                    for i in self.hashbucket[key][0]:
                        for j in self.hashbucket[key][1]:
                            if (i[0] == j[0] and i[1] != j[1] and i[3] != j[3]):
                                f.write(f"{i[0]} {i[2]} {j[2]}\n")
                                
    def shufflesort(self, file):
        """function to sort the intermediate key-value pairs by key and
        group the values that belong to the same key.
        """
        # check if file exists [no work for reducer]
        if not os.path.isfile(file):
            return
        with open(file, "r") as f:
            for line in f:
                input_list = line.strip().split(" ")   
                inner_list = eval(''.join(input_list[1:]))
                output_list = [[name.strip("'"), table.strip("'"), age] for name, table, age in inner_list]
                key = input_list[0]
                if key not in self.hashbucket:
                    self.hashbucket[key] = []
                self.hashbucket[key].append(output_list)  # mostly 1 since no local reduce
