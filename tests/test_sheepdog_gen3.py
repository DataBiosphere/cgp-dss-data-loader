import json
import os
import unittest
import uuid

from pathlib import Path

from tests import message
from transformer.gen3_transformer import main as transformer_main


class TestSheepdogGen3Transforming(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_path = Path(__file__).parents[0]
        cls.test_file = cls.test_path / 'test_data/topmed-public.json'
        cls.out_file = cls.test_path / f'{str(uuid.uuid4())}.tmp.json'

    def setUp(self):
        message('Make sure the output file doesn\'t exist yet')
        with self.assertRaises(FileNotFoundError):
            with open(self.out_file, 'r'):
                pass

    def _validate_output(self):
        message('Make sure that the output file was actually created')
        os.path.isfile(str(self.out_file))

        with open(self.test_path / 'test_data/transformer_sample_output.json', 'r',) as fp:
            valid_output = json.load(fp)
        valid_bundle_did = valid_output[0]['bundle_did']
        valid_output = valid_output[0]

        with open(self.out_file, 'r') as fp:
            test_output = json.load(fp)
        # since bundle did is changed each time the transformer runs, just normalize it for comparison
        for bundle in test_output:
            bundle['bundle_did'] = valid_bundle_did
        self.assertTrue(valid_output in test_output)

    def test_sheepdog_gen3_transforming(self):
        message('Run the transformer on sheepdog\'s output')
        argv = [str(self.test_file), '--output-json', str(self.out_file)]
        transformer_main(argv)

        self._validate_output()

    def tearDown(self):
        message('Clean up the output file if there is one')
        try:
            os.remove(self.out_file)
        except FileNotFoundError:
            pass
