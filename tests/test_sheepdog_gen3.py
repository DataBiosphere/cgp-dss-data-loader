import os
import unittest
import uuid

from pathlib import Path

from tests import message
from transformer.transform import main


class TestSheepdogGen3Transforming(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.project_path = Path(__file__).parents[1]
        cls.test_file = f'{cls.project_path}/tests/test_data/topmed-public.json'
        cls.out_file = f'{cls.project_path}/tests/{str(uuid.uuid4())}.tmp.json'

    def setUp(self):
        message('Make sure the output file doesn\'t exist yet')
        with self.assertRaises(FileNotFoundError):
            open(self.out_file, 'r')

    def test_sheepdog_gen3_transorming(self):
        message('Run the transformer on sheepdog\'s output')
        args = [self.test_file, '--output-json', self.out_file]
        main(args)

    def tearDown(self):
        message('Clean up the output file if there is one')
        try:
            os.remove(self.out_file)
        except FileNotFoundError:
            pass
