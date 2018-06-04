import os
import unittest
import uuid

from pathlib import Path

from tests import message
from transformer.transform import main as transformer_main


class TestSheepdogGen3Transforming(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.project_path = Path(__file__).parents[1]
        cls.test_file = f'{cls.project_path}/tests/test_data/topmed-public.json'
        cls.out_file = f'{cls.project_path}/tests/{str(uuid.uuid4())}.tmp.json'

    def setUp(self):
        message('Make sure the output file doesn\'t exist yet')
        with self.assertRaises(FileNotFoundError):
            with open(self.out_file, 'r'):
                pass

    def _validate_output(self):
        message('Make sure that the output file was actually created')
        with self.assertRaises(FileExistsError):
            with open(self.out_file, 'x'):
                pass

        # TODO: maybe make a json schema and test our output against it

    def test_sheepdog_gen3_transforming(self):
        message('Run the transformer on sheepdog\'s output')
        argv = [self.test_file, '--output-json', self.out_file]
        transformer_main(argv)

        self._validate_output()

    def tearDown(self):
        message('Clean up the output file if there is one')
        try:
            os.remove(self.out_file)
        except FileNotFoundError:
            pass
