from typing import List
from pandas.api.types import CategoricalDtype
import pandas as pd
import numpy as np

from ndop.preprocessing import list_size
from ndop import utils

# Measures for NDOP groups in age_gen_csv
NDOP_MEASURES = [
    ["ACH_DATE", "AGE_BAND", "GENDER"],
    ["ACH_DATE", "GENDER"],
    ["ACH_DATE", "AGE_BAND"],
    ["ACH_DATE"],
]

NDOP_DECEASED_MEASURES = [["ACH_DATE", "GENDER"], ["ACH_DATE"]]


def convert_gender_column_to_categorical(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts GENDER column in dataframe to categorical type to enable custom sort.

    Args:
        df (pd.DataFrame): NDOP aggregated counts containing GENDER groups.

    Returns:
        pd.DataFrame: NDOP dataframe with GENDER group converted to categorical type.
    """
    gender_column_order = CategoricalDtype(
        ["All", "All deceased", "Female", "Male", "Unknown / Prefer not to say"],
        ordered=True,
    )

    df["GENDER"] = df["GENDER"].astype(gender_column_order)

    return df


def convert_age_column_to_categorical(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts age column to categorical type in order to set custom sort order for age.

    Args:
        df (pd.DataFrame): DataFrame containing NDOP data and AGE_BAND column.

    Returns:
        pd.DataFrame: DataFrame with AGE_BAND column converted to ordered categorical type.
    """
    age_band_column_orders = CategoricalDtype(
        [
            "0-9",
            "10-19",
            "20-29",
            "30-39",
            "40-49",
            "50-59",
            "60-69",
            "70-79",
            "80-89",
            "90+",
            "All",
            "Unknown",
            "All deceased",
        ],
        ordered=True,
    )

    df["AGE_BAND"] = df["AGE_BAND"].astype(age_band_column_orders)

    return df


def add_age_or_gender_columns(df: pd.DataFrame, fill_value: dict) -> pd.DataFrame:

    """
    Helper function which creates dummy AGE_BAND or GENDER column if not present in dataframe and fills with value.

    Args:
        df: DataFrame containing NDOP aggregated values.
        fill_value: Column value to set if column is created.
    Returns:
        pd.DataFrame: DataFrame containing NDOP data with additional columns.
    """

    demographic_cols = ["AGE_BAND", "GENDER"]

    df = df.reindex(df.columns.union(demographic_cols, sort=False), axis=1).fillna(
        fill_value
    )

    return df


def aggregate_ndop_by_measure(
    df: pd.DataFrame, measures: List[str], fill_value: str
) -> pd.DataFrame:

    """
    Generic measure for purpose of aggregating NDOP counts according to demographic measures parsed.
    Measures should be presented as list with at least one demographic.

    Args:
        df(pd.DataFrame): DataFrame containing NDOP data.
        measures(List): Demographic columns on which to aggregate data.
        fill_value(str): Fill value for columns where values are Null.

    Returns:
        pd.DataFrame: Aggregated NDOP counts for parsed measures.
    """

    df = (
        df.groupby(measures)["NHS_Number"]
        .count()
        .reset_index()
        .rename(columns={"NHS_Number": "OPT_OUT"})
        .pipe(add_age_or_gender_columns, fill_value)
    )

    return df


def calculate_opt_out_rate(df: pd.DataFrame) -> pd.DataFrame:
    df["OPT_OUT_RATE"] = 100 * (df["OPT_OUT"] / df["LIST_SIZE"])

    return df


def fill_list_size_and_opt_out_nan_values(df: pd.DataFrame) -> pd.DataFrame:
    df["OPT_OUT_RATE"] = df["OPT_OUT_RATE"].replace(np.nan, 0).astype(float)
    df["LIST_SIZE"] = df["LIST_SIZE"].replace(np.nan, 0).astype(int)

    return df


def suppress_unknown_undisclosed_gender_rows(df: pd.DataFrame) -> pd.DataFrame:

    """
    Function which removes the Unknown gender groups which are aggregated by known age band.
    These rows are not present in output.

    Args:
        df(pd.DataFrame): DataFrame containing NDOP data for age_gen_csv grouped by gender and age-band.

    Returns:
        pd.DataFrame: DataFrame with specified supressed rows removed.
    """

    undisclosed_gender = df["GENDER"] == "Unknown / Prefer not to say"
    not_all_age_or_deceased = df["AGE_BAND"].isin(["All", "All deceased"]) == False

    df = df.drop(df[(undisclosed_gender) & (not_all_age_or_deceased)].index)

    return df


def create_df_list_for_age_gen_csv(
    df: pd.DataFrame, measure_groups: List, fill_value: str
) -> List[pd.DataFrame]:
    """
    High level function, loops through each measure grouping in list and aggregate records based on each measure grouping.
    Each dataframe is then stored in a list.

    Args:
        df(pd.DataFrame): DataFrame containing NDOP records for all months.
        measure_groups(List): List of demographic measure groupings that are required for the age_gen_csv output.
        fill_value(str): Fill value for columns if value is Null.

    Returns:
        List[pd.DataFrame]: List of dataframes, counts aggregated on each combination of measure.
    """

    ndop_dfs = [
        aggregate_ndop_by_measure(df, measure, fill_value=fill_value)
        for measure in measure_groups
    ]

    return ndop_dfs


def create_age_gen_csv(
    ndop_df: pd.DataFrame,
    ndop_deceased_df: pd.DataFrame,
    list_size_df: pd.DataFrame,
) -> pd.DataFrame:

    """
    Generates all breakdowns of for age_gen csv.

    Args:
        ndop_df(pd.DataFrame): DataFrame contain active (living) patient NDOP records for each month of reporting period.
        ndop_deceased_df(pd.DataFrame): DataFrame containing deceased patient records for each month of reporting period.
        list_size_df(pd.DataFrame): DataFrame containing list size aggregated by age band, gender and achievement date for each month of reporting period.

    Returns:
        pd.DataFrame: DataFrame containing aggregated NDOP output for age_gen_csv.
    """
    ndop_dfs = create_df_list_for_age_gen_csv(ndop_df, NDOP_MEASURES, fill_value="All")
    ndop_deceased_dfs = create_df_list_for_age_gen_csv(
        ndop_deceased_df, NDOP_DECEASED_MEASURES, fill_value="All deceased"
    )
    list_size_for_age_gen_df = list_size.list_size_for_age_gen_csv(list_size_df)

    output = (
        pd.concat([*ndop_dfs, *ndop_deceased_dfs])
        .merge(
            list_size_for_age_gen_df, on=["ACH_DATE", "AGE_BAND", "GENDER"], how="left"
        )
        .pipe(calculate_opt_out_rate)
        .pipe(fill_list_size_and_opt_out_nan_values)
        .pipe(convert_gender_column_to_categorical)
        .pipe(convert_age_column_to_categorical)
        .pipe(suppress_unknown_undisclosed_gender_rows)
        .sort_values(["ACH_DATE", "GENDER", "AGE_BAND"], ascending=[False, True, True])
        .pipe(utils.format_publication_date)
    )

    return output
