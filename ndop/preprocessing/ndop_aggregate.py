from typing import Callable

import pandas as pd
import numpy as np
from ndop.config import config, params
from ndop.preprocessing import ndop_clean


def filter_living_patients(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    Keeps records in dataframe if patient is alive on date parsed.

    Returns:
        pd.DataFrame: DataFrame containing living patients on reporting date.
    """

    null_date_of_death = df["DATE_OF_DEATH"].isna()
    rped_before_date_of_death = df["DATE_OF_DEATH"] > date

    return df[(null_date_of_death) | (rped_before_date_of_death)]


def filter_deceased_patients(df: pd.DataFrame, date: str) -> pd.DataFrame:
    return df.query(f"DATE_OF_DEATH <= '{date}'")


def retrieve_most_recent_record(df: pd.DataFrame) -> pd.DataFrame:

    """
    Retrieves most recent record for patients with multiple records.
    Function works in similar fashion to SQL window rank methods.

    Args:
        df: Data with NDOP records.

    Returns:
        pd.DataFrame: DataFrame with most recent records for each NHS Number.
    """

    # Ranks records by most recent for each NHS Number.
    df["Row Num"] = df.groupby(["NHS_Number"])["Record_Start_Date"].rank(
        method="first", ascending=False
    )

    df = df[df["Row Num"] == 1].drop(columns=["Row Num"]).reset_index(drop=True)

    # logger.info("Most recent records for each NHS number retrieved.")

    return df


def slice_ndop_by_month(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    Slices NDOP dataframe, taking records that are active by comparing date against record start date and record end date.
    Adds parsed date as column 'ACH DATE' for grouping data by month later.

    Args:
        df (pd.DataFrame): NDOP records data.
        date (str): Date to check for active records.

    Returns:
        pd.DataFrame: NDOP records that are active on date.
    """
    df = df[
        (df["Record_Start_Date"] <= date)
        & ((df["Record_End_Date"] >= date) | (df["Record_End_Date"].isna()))
    ].copy()

    df["ACH_DATE"] = date

    return df


def concatenate_ndop_monthly_records(
    ndop_record_type_function: Callable, df: pd.DataFrame, report_dt: config.reportDates
):

    """
    Slices NDOP that are active for each month of reporting period and concatenates into single dataframe.

    Returns:
        pd.DataFrame: Concatenated Dataframe of NDOP records for each month of reporting period.
    """
    ndop_monthly_dfs = [
        ndop_record_type_function(df, date)
        for date in report_dt.get_reporting_months_list()
    ]

    ndop_data_concatenated_df = pd.concat(ndop_monthly_dfs)

    return ndop_data_concatenated_df


def process_active_ndop_records(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    High level function which finds active NDOP records at dates parsed and removes deceased patients.

    Args:
        df (pd.DataFrame): NDOP dataframe.
        date (str): Date on which record should be active.

    Returns:
        pd.DataFrame: Monthly Slice of active NDOP records.
    """
    df = (
        df.pipe(slice_ndop_by_month, date)
        .pipe(filter_living_patients, date)
        .pipe(retrieve_most_recent_record)
    )

    return df


def process_deceased_ndop_records(df: pd.DataFrame, date: str) -> pd.DataFrame:

    df = (
        df.pipe(slice_ndop_by_month, date)
        .pipe(filter_deceased_patients, date)
        .pipe(retrieve_most_recent_record)
    )

    return df


def preprocess_ndop_data(report_dates):

    ndop_connection = config.create_sql_connection(params.NDOP_CONNECTION_STRING)
    ndop_df = ndop_clean.get_ndop_data(report_dates, ndop_connection)

    # NDOP data needs to be separated into active records and deceased patients
    ndop_concat_df = concatenate_ndop_monthly_records(
        process_active_ndop_records, ndop_df, report_dates
    )
    ndop_deceased_concat_df = concatenate_ndop_monthly_records(
        process_deceased_ndop_records, ndop_df, report_dates
    )

    return ndop_concat_df, ndop_deceased_concat_df


def flag_active_record_per_month(df: pd.DataFrame, report_date: str) -> pd.DataFrame:

    active_ndop_record = (df["Record_Start_Date"] <= report_date) & (
        (df["Record_End_Date"] >= report_date) | (df["Record_End_Date"].isna())
    )

    null_date_of_death = df["DATE_OF_DEATH"].isna()
    rped_before_date_of_death = df["DATE_OF_DEATH"] > report_date

    df[f"is_active_on_{report_date}"] = np.where(
        active_ndop_record & (null_date_of_death | rped_before_date_of_death), 1, 0
    )

    df["Row Num"] = df.groupby(["NHS_Number"])["Record_Start_Date"].rank(
        method="first", ascending=False
    )

    return df


def aggregate_ndop_by_practice(ndop_df: pd.DataFrame, rename_col: str = 'OPT_OUT') -> pd.DataFrame:
    """
    Aggregates NDOP counts by Practice Code and CCG in preparation for reg_geog_csv.

    Args:
        ndop_df (pd.DataFrame): Cleaned NDOP data.

    Returns:
        pd.DataFrame: Returns counts of NDOP by practice and CCG residence.
    """
    df = (
        ndop_df.groupby(["ACH_DATE", "GP_PRACTICE"])["NHS_Number"]
        .count()
        .reset_index()
        .rename(columns={"NHS_Number": f"{rename_col}"})
    )

    return df


def aggregate_ndop_by_lsoa(ndop_df: pd.DataFrame) -> pd.DataFrame:

    df = (
        ndop_df.groupby(["ACH_DATE", "LSOA_CODE"])["NHS_Number"]
        .count()
        .reset_index()
        .rename(columns={"NHS_Number": "OPT_OUT"})
    )

    return df


def aggregate_ndop_by_sub_icb(ndop_df: pd.DataFrame, rename_column: str = "OPT_OUT") -> pd.DataFrame:

    df = (
        ndop_df.groupby(["ACH_DATE", "SUB_ICB_LOCATION_CODE"])["NHS_Number"]
        .count()
        .reset_index()
        .rename(
            columns={"NHS_Number": f"{rename_column}", "REGISTERED_CCG": "SUB_ICB_LOCATION_CODE"}
        )
    )

    return df
