import unittest
from pipeline.etl import SimpleTransformer
import pandas as pd


class TestTransform(unittest.TestCase):
        def test_transform_data(self):
            transformed = SimpleTransformer()
            input_df = pd.DataFrame({
                "id": [1234, 2345],
                "ammattiala": ["IT", "Art"],
                "tyotehtava": ["Developer", "QA"],
                "tyoavain": ["1234", "5678"],
                "osoite": ["123 Street", "456 Avenue"],
                "haku_paattyy_pvm": ["2024-12-20", "2024-12-01"],
                "x": [24.935, 25.015],
                "y": [60.169, 60.192],
                "linkki": ["http://job1", "http://job2"],
                "irrelevant_column": ["drop1", "drop2"],  # Extra column to be dropped
            })
            renamed_df = SimpleTransformer()._rename_columns(input_df) #Rename based on the defined schema and dropped 'irrelevant fields'.
            #print("new list:", renamed_df)


            expected_renamed_df = pd.DataFrame({
                "id": [1234, 2345],
                "ammattiala": ["IT", "Art"],
                "job_title": ["Developer", "QA"],
                "job_key": ["1234", "5678"],
                "address": ["123 Street", "456 Avenue"],
                "application_end_date": ["2024-12-20", "2024-12-01"],  # Still strings, not yet dates
                "longitude_wgs84": [24.935, 25.015],
                "latitude_wgs84": [60.169, 60.192],
                "link": ["http://job1", "http://job2"],
            })

            # Validate the renamed DataFrame matches the expected output
            pd.testing.assert_frame_equal(renamed_df, expected_renamed_df)


            # validate the 'irrevelevant_column' dropped from the list
            self.input_df = renamed_df.rename(columns=transformed.rename_schema)
            transformed_df = transformed._transform_dates(self.input_df)
            #print("Columns in result_df:", transformed_df.columns)
            #print("Columns in expected_renamed_df:", expected_renamed_df.columns)
            self.assertListEqual(renamed_df.columns.tolist(), expected_renamed_df.columns.tolist())


        #  Validate Transfrom dates from strings to date objects and Ensure consistent data types for comparison
            new_transformed_df = transformed(input_df)
            #print("Transformed DataFrame dtypes:", transformed_df.dtypes)
            #print("Expected DataFrame dtypes:", expected_renamed_df.dtypes)
            new_transformed_df["application_end_date"] = new_transformed_df["application_end_date"].astype(str)
            expected_renamed_df["application_end_date"] = expected_renamed_df["application_end_date"].astype(str)
            pd.testing.assert_frame_equal(new_transformed_df, expected_renamed_df)


if __name__ == '__main__':
    unittest.main()
