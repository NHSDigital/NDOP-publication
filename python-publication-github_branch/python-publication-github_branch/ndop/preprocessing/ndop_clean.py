from re import sub
from ndop.config import config, params
import pandas as pd


def get_ndop_data(dates: config.reportDates, connection) -> pd.DataFrame:

    """
    Pipeline for extracting NDOP data from SQL data base and processing before splitting into
    active NDOP records and deceased patients.

    Returns:
        pd.DataFrame: DataFrame containing process NDOP records.
    """

    df = (
        config.get_ndop_data(dates, connection)
        .pipe(clean_nhs_numbers)
        .pipe(remove_practice_code_whitespace)
        .pipe(remap_ndop_age_column)
        .pipe(remap_ndop_gender_column)
    )

    return df


# NDOP cleaning funcs


def clean_nhs_numbers(df: pd.DataFrame) -> pd.DataFrame:

    """
    Function which removes invalid NHS numbers from NDOP data.

    Args:
        df: DataFrame containing NDOP data.

    Returns:
        df: cleaned Dataframe with valid NDOP rows.
    """

    df = (
        df.pipe(remove_nhs_number_which_are_invalid)
        .pipe(remove_nhs_number_starting_with_9)
        .pipe(remove_nhs_number_which_are_null)
        .pipe(fill_empty_categorical_column_values)
        .pipe(fill_empty_gender_column_values)
    )

    return df


def remove_nhs_number_starting_with_9(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["NHS_Number"].str[0] != "9"]


def remove_nhs_number_which_are_null(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["NHS_Number"].notna()]


def remove_nhs_number_which_are_invalid(df: pd.DataFrame) -> pd.DataFrame:
    return df[~df["NHS_Number"].isin(params.INVALID_NHS_NUMBERS)]


def fill_empty_categorical_column_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills null values in categorical columns with assigned values.
    """
    cols = [
        "GP_PRACTICE",
        #"REGISTERED_CCG",
        #"CCG_OF_RESIDENCE",
        #"LOCAL_AUTHORITY",
        "LSOA_CODE",
    ]

    df[cols] = df[cols].fillna("Unallocated")

    return df


def fill_empty_gender_column_values(df: pd.DataFrame):

    df["GENDER"] = df["GENDER"].fillna("Unknown / Prefer not to say")

    return df


def remap_ndop_age_column(df: pd.DataFrame) -> pd.DataFrame:

    """
    Cleans and remaps the values in age column to match value names in outputs.

    Returns:
        pd.DataFrame: DataFrame containing remapped age values.
    """

    age_mappings = {"N/": "Unknown", "90 and over": "90+"}

    df["AGE_BAND"] = df["AGE_BAND"].str.strip("Age ")
    df["AGE_BAND"] = df["AGE_BAND"].replace(age_mappings)

    return df


def remap_ndop_gender_column(df):

    gender_mappings = {
        "1": "Male",
        "2": "Female",
        "0": "Unknown / Prefer not to say",
        "None": "Unknown / Prefer not to say",
        "9": "Unknown / Prefer not to say",
    }

    df["GENDER"] = df["GENDER"].replace(gender_mappings)

    return df


def remove_practice_code_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes whitespace from GP Practice Codes which may affect joining list size data later in pipeline.

    Args:
        df (pd.DataFrame): NDOP records dataframe containing GP_PRACTICE column.

    Returns:
        pd.DataFrame: NDOP records with cleaned GP Practice columns.
    """
    df["GP_PRACTICE"] = df["GP_PRACTICE"].str.strip()

    return df
