import unittest
from pipeline.etl import SimpleExtractor
import pandas as pd


class TestSimpleExtractor(unittest.TestCase): #test the extract data 
    def test_extract_from_local_file(self):
        extractor = SimpleExtractor()
         # Extract data
        df = extractor.extract()
        # Assertions; verifying the datas
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        self.assertIn('id', df.columns)
        self.assertIn('ammattiala', df.columns)
        self.assertIn('tyotehtava', df.columns)
        self.assertEqual(len(df), 51)

if __name__ == '__main__':
    unittest.main()