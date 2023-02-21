import datetime

import pandas as pd
import pytest
import ndop.excel.ndop_clean as clean
import ndop.config.config as config
from pandas.testing import assert_frame_equal


class TestNDOPCleaningFuncs: 

    @pytest.fixture
    def nhs_numbers(self):
        return pd.DataFrame({'NHS_Number': ['5555555555', '111111', '2222', '99999', None]})

    @pytest.fixture
    def date_of_death(self):
        return pd.DataFrame(
            {
                'NHS_Number':       ['1111',  '2222'],
                'DATE_OF_DEATH':    [datetime.date(2021,1,1),    None],
            }
        )

    @pytest.fixture
    def categorical_columns(self):
        return pd.DataFrame(
            {
                'GP_PRACTICE':      ['M12345',  None],
                'REGISTERED_CCG':   ['BF1',     None],
                'CCG_OF_RESIDENCE': ['BF1',     None],
                'LOCAL_AUTHORITY':  ['BF1',     None],
                'LSOA':             ['BF1',     None],
                'GENDER':           [0,         None]
            }
        )


    def test_ndop_removing_invalid_nhs_numbers_starting_with_9(self, nhs_numbers):
        
        actual = clean.remove_nhs_number_starting_with_9(nhs_numbers).reset_index(drop=True)

        expected = pd.DataFrame({'NHS_Number': ['5555555555', '111111', '2222', None]}).reset_index(drop=True)

        assert_frame_equal(actual, expected)


    def test_ndop_removing_invalid_nhs_numbers_defined_in_config(self, nhs_numbers):

        actual = clean.remove_nhs_number_which_are_invalid(nhs_numbers).reset_index(drop=True)

        expected = pd.DataFrame({'NHS_Number': ['111111', '2222', '99999', None]}).reset_index(drop=True)

        assert_frame_equal(actual, expected)


    def test_remap_columns_returns_correct_values(self, categorical_columns):

        actual = clean.fill_empty_categorical_column_values(categorical_columns)

        expected = pd.DataFrame(
            {
                'GP_PRACTICE':      ['M12345', 'Unallocated'],
                'REGISTERED_CCG':   ['BF1', 'Unallocated'],
                'CCG_OF_RESIDENCE':  ['BF1', 'Unallocated'],
                'LOCAL_AUTHORITY':   ['BF1', 'Unallocated'],
                'LSOA':             ['BF1', 'Unallocated'],
                'GENDER':           [0, None]
            }
        )

        assert_frame_equal(actual.reset_index(drop=True), expected.reset_index(drop=True))

    def test_empty_gender_columns_filled_with_unknown_in_none_values(self, categorical_columns):
    
        actual = clean.fill_empty_gender_column_values(categorical_columns)

        expected = pd.DataFrame(
            {
                'GP_PRACTICE':      ['M12345',  None],
                'REGISTERED_CCG':   ['BF1',     None],
                'CCG_OF_RESIDENCE': ['BF1',     None],
                'LOCAL_AUTHORITY':  ['BF1',     None],
                'LSOA':             ['BF1',     None],
                'GENDER':           [0,         "Unknown / Prefer not to say"]
            }
        )

        assert_frame_equal(actual.reset_index(drop=True), expected.reset_index(drop=True))
    

    def test_remove_deceased_patients_from_dataframe(self, date_of_death):

        actual = clean.remove_deceased_patients(date_of_death, datetime.date(2022, 6, 1))

        expected = pd.DataFrame(
            {
                'NHS_Number':       ['2222'],
                'DATE_OF_DEATH':    [None],
            }
        )

        assert_frame_equal(actual.reset_index(drop=True), expected.reset_index(drop=True))

        