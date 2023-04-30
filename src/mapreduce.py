import os


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
        # will generate n_reduce intermediate file_system

        for key, value in l_k_v:
            key_int = int(hash(key.encode()).hexdigest(), 16)
            # hash the key and mod it with n_reduce
            # write the key-value pair to the corresponding file
            self.file_handles[key_int % self.n_reduce][0].acquire()
            self.file_handles[key_int % self.n_reduce][1].write(f"{key} {value}\n")
            self.file_handles[key_int % self.n_reduce][0].release()

    def parse_map_loc(self, map_loc):
        """This function will read all assigned intermediate file_system of the mapper"""
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
        # will generate n_reduce intermediate file_system

        for key, value in l_k_v:
            key_int = int(hash(key.encode()).hexdigest(), 16)
            # hash the key and mod it with n_reduce
            # write the key-value pair to the corresponding file
            self.file_handles[key_int % self.n_reduce][0].acquire()
            self.file_handles[key_int % self.n_reduce][1].write(f"{key}\t{value}\n")
            self.file_handles[key_int % self.n_reduce][0].release()

    def parse_map_loc(self, map_loc):
        """This function will read all assigned intermediate file_system of the mapper"""
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
                key, value = line.strip().split("\t")
                if key not in self.hashbucket:
                    self.hashbucket[key] = set()
                self.hashbucket[key].add(value)


class NJ:
    def parse_and_map(self, file):
        with open(file, "r") as f:
            lines = f.readlines()
        headers = lines[0].strip().split(", ")
        common_header = headers[0]
        # print("file", file)
        dir_name, file_name = os.path.split(file)
        # file_name = file_name.split("_")[0]
        for line in lines[1:]:
            header_val_1, header_val_2 = line.strip().split(", ")
            if header_val_1 not in self.saver:
                self.saver[header_val_1] = []
            self.saver[header_val_1].append(
                [common_header, header_val_2, file_name, headers[1]]
            )
        self.partition(self, self.saver)

    def map(**args):
        return None

    def partition(self, mapping_dict):
        from hashlib import md5

        hash = md5
        # will generate n_reduce intermediate file_system
        for key in mapping_dict:
            key_int = int(hash(key.encode()).hexdigest(), 16)
            # hash the key and mod it with n_reduce
            # write the key-value pair to the corresponding file
            self.file_handles[key_int % self.n_reduce][0].acquire()
            # Writes in the form of "header -> rest" : eg. Name -> [Name, file, Age value, "Age"]
            # Age -> [Age, file, Name value, "Name"]
            self.file_handles[key_int % self.n_reduce][1].write(
                f"{key} {mapping_dict[key]}\n"
            )
            self.file_handles[key_int % self.n_reduce][0].release()

    def parse_map_loc(self, map_loc):
        """This function will read all assigned intermediate file_system of the mapper"""
        # print("Location", map_loc, "Node", self.node_name)
        for file in os.listdir(map_loc):
            self.shufflesort(self, os.path.join(map_loc, file, self.node_name))
        # print("Check", self.hashbucket)

        def joinn(key, value):
            list_a = []
            list_b = []
            for x in value:
                for val in x:
                    table_check = val[2]
                    if (
                        table_check == "input1_table1.txt"
                        or table_check == "input2_table1.txt"
                    ):
                        list_a.append(val[1])
                    else:
                        list_b.append(val[1])

            final_ans = []

            list_a = list(set(list_a))
            list_b = list(set(list_b))
            for t in list_a:
                for u in list_b:
                    temp = [key, t, u]
                    final_ans.append(temp)

            return final_ans

        first_name = None
        second_name = None
        third_name = None
        for x in self.hashbucket:
            temp_lis = self.hashbucket[x]
            if len(temp_lis):
                first_name = temp_lis[0][0][0]
                if (
                    temp_lis[0][0][2] == "input1_table2.txt"
                    or temp_lis[0][0][2] == "input2_table2.txt"
                ):
                    third_name = temp_lis[0][0][3]
                else:
                    second_name = temp_lis[0][0][3]
            if (
                first_name is not None
                and second_name is not None
                and third_name is not None
            ):
                break

        lis = []
        if (
            first_name is not None
            and second_name is not None
            and third_name is not None
        ):
            lis.append(first_name)
            lis.append(second_name)
            lis.append(third_name)

        with open(f"{self.output_dir}/Output{self.node_name}.txt", "w") as f:
            if len(lis) == 3:
                f.write(", ".join(lis))
                f.write("\n")
                for x in self.hashbucket:
                    final_res = joinn(x, self.hashbucket[x])
                    for var in final_res:
                        if len(var):
                            f.write(", ".join(var))
                        f.write("\n")

        self.reduce(self)

    # will only be called when IF received from all mappers
    def reduce(self):
        """function to reduce the values that belong to the same key."""
        pass

    def shufflesort(self, file):
        """function to sort the intermediate key-value pairs by key and
        group the values that belong to the same key.
        """
        # check if file exists [no work for reducer]
        if not os.path.isfile(file):
            return
        with open(file, "r") as f:
            # print("FILE", file, "NODE", self.node_name)
            for line in f:
                # reading and converting it into a list
                input_list = line.strip().split(" ")
                # print(input_list, "SUGGONDEnuefeifjZ")
                inner_list = eval("".join(input_list[1:]))
                # print(inner_list, "RETARDA", inner_list[0])
                output_list = [
                    [name.strip(), table.strip(), age, agelabel]
                    for name, table, age, agelabel in inner_list
                ]
                # print("COULDNT OUTPUT", output_list)
                key = input_list[
                    0
                ]  # key is the header eg. Name -> list in parse_and_map
                # print("KEY", key)
                if key not in self.hashbucket:
                    self.hashbucket[key] = []
                self.hashbucket[key].append(
                    output_list
                )  # mostly 1 since no local reduce
                # print("HASHBUCKET", self.hashbucket, "NODE", self.node_name)
                # common column will have 2 lists in the list eg. [[Name, file, Age value, "Age"], [Name, file, Role value, "Role"]]
