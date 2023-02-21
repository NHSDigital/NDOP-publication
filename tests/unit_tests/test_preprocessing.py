from ndop.preprocessing import ndop_clean

import pandas as pd
from pandas.testing import assert_frame_equal
import datetime
import pytest


class TestNDOPCleaningFunctions:
    @pytest.fixture
    def ndop_records(self):

        return pd.DataFrame(
            {
                "NHS_Number": ["12345", "12345", "12345", "12345"],
                "Record_Start_Date": [
                    datetime.date(2022, 11, 1),
                    datetime.date(2020, 1, 1),
                    datetime.date(2021, 1, 5),
                    datetime.date(2023, 1, 1),
                ],
                "DATE_OF_DEATH": [
                    "2021-01-01",
                    "2021-01-01",
                    "2021-01-02",
                    "2022-01-01",
                ],
            }
        )

    def test_retrieving_most_recent_ndop_record(self, ndop_records):

        expected = pd.DataFrame(
            {
                "NHS_Number": ["12345"],
                "Record_Start_Date": [datetime.date(2023, 1, 1)],
                "DATE_OF_DEATH": ["2022-01-01"],
            }
        ).reset_index(drop=True)

        actual = ndop_clean.retrieve_most_recent_record(ndop_records).reset_index(
            drop=True
        )

        assert_frame_equal(actual, expected)

    def test_retrieving_most_recent_record_for_deceased_patients(self, ndop_records):

        expected = pd.DataFrame(
            {
                "NHS_Number": ["12345"],
                "Record_Start_Date": [datetime.date(2023, 1, 1)],
                "DATE_OF_DEATH": ["2022-01-01"],
            }
        ).reset_index(drop=True)

        actual = ndop_clean.retrieve_deceased_patients(
            ndop_records, "2022-09-01"
        ).reset_index(drop=True)

        assert_frame_equal(actual, expected)
