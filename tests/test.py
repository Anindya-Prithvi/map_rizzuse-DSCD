"""This file contains the tests for src/master.py"""

import shutil
import sys
import time
import unittest

from loguru import logger

sys.path.append("src")
from master import Master


class TestMaster(unittest.TestCase):
    def test01_setup_destroy(self):
        logger.level("DEBUG")
        logger.debug("Testing master node setup.")
        master = Master("input_data", "output_data", 2, 2, "WC", "map_intermediate")
        assert master.input_data == "input_data"
        assert master.output_data == "output_data"
        assert master.n_map == 2
        assert master.n_reduce == 2

        master.destroy_nodes()
        master.server.stop(0)
        time.sleep(0.5)

        shutil.rmtree("map_intermediate")
        shutil.rmtree("output_data")
        input("Press enter to start word count test.")

    def test02_wc(self):
        logger.level("DEBUG")
        logger.debug("Testing master node setup.")
        master = Master(
            "src/samples/word_count/in",
            "output_data_wordcount",
            3,
            2,
            "WC",
            "map_intermediate_wordcount",
        )
        master.run()
        master.destroy_nodes()
        time.sleep(0.5)
        master.server.stop(0)
        input("Press enter to start inverted index test.")
