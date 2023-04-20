class Mapper:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def map(self):
        with open(self.input_file, 'r') as f:
            for line in f:
                self.map_line(line)