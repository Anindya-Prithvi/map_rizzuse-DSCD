"""This file contains the tests for src/master.py"""

import unittest

from loguru import logger

from src.master import Master


class TestMaster(unittest.TestCase):
    def testsetup(self):
        logger.level("DEBUG")
        logger.debug("Testing master node setup.")
        master = Master("input_data", "output_data", 2, 2)
        assert master.input_data == "input_data"
        assert master.output_data == "output_data"
        assert master.n_map == 2
        assert master.n_reduce == 2
