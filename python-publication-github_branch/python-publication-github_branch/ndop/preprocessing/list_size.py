from typing import List
import pandas as pd
import re
import ndop.config.config as config
import ndop.config.params as params
import sqlalchemy
from ndop.preprocessing import ingest_icb

def get_list_size_for_report(
    report_date: config.reportDates, sql_connection: sqlalchemy.engine.base.Engine
) -> pd.DataFrame:

    """
    High level function for creating list size data for entire publication.
    List Size is extracted from SQL, transformed to wide to long, joined with age ref columns and subsetted to show active practices only.

    Args:
        report_date: reportDate object covering reporting period.
        sql_connection: sqlalchemy connection to sql database.

    Returns:
        pd.DataFrame: List Size dataframe by age, gender, practice details and reporting month.
    """

    ndop_dates = report_date.get_reporting_months_list()

    list_size_data = config.get_list_size_data(report_date, sql_connection)
    male_female_cols = retrieve_male_female_columns(list_size_data)

    list_size_df = melt_list_size_data(list_size_data, male_female_cols)
    active_practice_by_month_df = find_active_practice_by_month(
        sql_connection, ndop_dates
    )

    # Ref table for joining on ages
    age_columns = config.create_age_cols()

    list_size_joined = list_size_df.merge(
        age_columns, on=["AGE_GEN"], how="left"
    ).merge(active_practice_by_month_df, on=["GP_PRACTICE", "ACH_DATE"], how="inner")

    return list_size_joined


def preprocess_list_size(report_date: config.reportDates) -> pd.DataFrame:
    """
    Prepares list size for use with other functions in publication.

    Args:
        report_date (config.reportDates): report_dates object with dates for publication.

    Returns:
        pd.DataFrame: List Size dataframe by age and practice by reporting month.
    """

    connection = config.create_sql_connection(params.DSS_CORP_CONNECTION_STRING)
    list_size = get_list_size_for_report(report_date, connection)

    return list_size


def retrieve_male_female_columns(list_size_df: pd.DataFrame) -> List[str]:
    """
    Extracts all columns that contain the MALE or FEMALE in column names into List.

    Args:
        list_size_df(df): DataFrame containing List Size data.

    Returns:
        List[str]: List of columns containing male or female patient counts.
    """

    return [
        column
        for column in list_size_df.columns
        if re.match(r"(FE)*MALE_\d{1,3}_\d{1,3}", column)
    ]

def merge_prac_icb_mapping(
    to_map_df: pd.DataFrame, reference_df: pd.DataFrame, geog_type: str
) -> pd.DataFrame:
    """
    Function that can be called in sequence to join each geography type into to a different column on a df

    Args:
        to_map_df (pd.DataFrame): df to add geography column to
        reference_df (pd.DataFrame): long df containing data of each geography stacked
        geog_type (str): geog type col to add

    Returns:
        pd.DataFrame: df with column of geog type specificed added 
    """
    mapping_df = (
        pd.merge(
            to_map_df,
            reference_df,
            left_on = f"{geog_type}_CODE",
            right_on = "DH_GEOGRAPHY_CODE",
            how = "left"
        )
        .drop(
            columns = [
            "DATE_OF_OPERATION", 
            "DH_GEOGRAPHY_CODE"
            ]
        )
        .rename(
            columns={
            "DH_GEOGRAPHY_NAME":f"{geog_type}_NAME",
            "GEOGRAPHY_CODE":f"ONS_{geog_type}_CODE"
            }
        )
    )

    return mapping_df

def get_active_practices(
    conn: sqlalchemy.engine.base.Engine, date: str
) -> pd.DataFrame:

    """
    Find most recent record for active practices that are active on parsed date.

    Args:
        conn: connection for accessing SQL database.
        date: date on which to take practice active status snapshot

    Returns:
        DataFrame containing active practices and their related geographies.
    """

    practice_mapping_query = f"""SELECT a.[CODE] AS GP_PRACTICE
                                     , a.[NAME] AS PRACTICE_NAME
                                     , a.[POSTCODE] AS POSTCODE
                                     , a.[COMMISSIONER_ORGANISATION_CODE] AS SUB_ICB_LOCATION_CODE
                                     , a.[HIGH_LEVEL_HEALTH_GEOGRAPHY] AS ICB_CODE
                                     , a.[NATIONAL_GROUPING] AS COMM_REGION_CODE
                                     , CAST('{date}' AS DATE) AS ACH_DATE 
                                FROM [DSS_CORPORATE].[dbo].[ODS_PRACTICE_V02] as a
                                WHERE a.OPEN_DATE <= '{date}' 
                                AND (a.CLOSE_DATE IS NULL OR a.CLOSE_DATE >= '{date}')
                                AND a.DSS_RECORD_START_DATE <= '{date}'
                                AND (a.DSS_RECORD_END_DATE IS NULL OR a.DSS_RECORD_END_DATE >= '{date}')
                                AND a.CODE IN (SELECT DISTINCT CODE FROM [DSS_CORPORATE].[dbo].[GP_PATIENT_LIST] WHERE EXTRACT_DATE = '{date}')
                                ORDER BY a.CODE"""

    active_pracs_df = pd.read_sql(practice_mapping_query, conn)
    
    icb_mapping_df = ingest_icb.get_icb_mapping(date)
    
    prac_icb_mapped_df = (
        active_pracs_df
        .pipe(merge_prac_icb_mapping, reference_df=icb_mapping_df, geog_type='SUB_ICB_LOCATION')
        .pipe(merge_prac_icb_mapping, reference_df=icb_mapping_df, geog_type='ICB')
        .pipe(merge_prac_icb_mapping, reference_df=icb_mapping_df, geog_type='COMM_REGION')
    )

    return prac_icb_mapped_df


def find_active_practice_by_month(list_size_conn, dates: list[str]) -> pd.DataFrame:

    """
    Finds practices which are active for each date in dates list and concatenates into single dataframe.

    Args:
        dates: list of dates containing months covering reporting period

    Returns:
        DataFrame of concatentated active practices for each month.
    """

    dfs = [get_active_practices(list_size_conn, month) for month in dates]
    joined_dfs = pd.concat(dfs)

    return joined_dfs


def melt_list_size_data(
    list_size_df: pd.DataFrame, age_gen_columns: list[str]
) -> pd.DataFrame:

    """
    Melt list size age and gender columns of dataframe to create long format ready for aggregation.

    Args:
        list_size_df: DataFrame containing list size data.
        age_gen_columns: List of columns containing Male and Female patient counts.

    Returns:
        Long format of list size data.
    """

    df = pd.melt(
        list_size_df,
        id_vars=["GP_PRACTICE", "ACH_DATE"],
        value_vars=[*age_gen_columns],
        var_name="AGE_GEN",
        value_name="LIST_SIZE",
    )

    return df


# Formatting functions


def remap_gender_column(df: pd.DataFrame) -> pd.DataFrame:

    df["GENDER"] = df["GENDER"].str.capitalize()

    return df


def remap_age_band(df: pd.DataFrame) -> pd.DataFrame:

    df["AGE_BAND"] = df["AGE_BAND"].str.replace("_", "-")

    return df


def aggregate_list_size_by_measure(df: pd.DataFrame, measure: List) -> pd.DataFrame:
    """
    Generic function for aggregating list size by one or more demographic measures.

    Args:
        df (pd.DataFrame): DataFrame containing disaggregated list size data.
        measure (List): Measure(s) to group list size by

    Returns:
        pd.DataFrame: Aggregated list size count by measure.
    """

    return df.groupby(measure)["LIST_SIZE"].sum().reset_index()


# For use in Excel Table 1
def aggregate_list_size_by_month(df: pd.DataFrame) -> pd.DataFrame:

    return df.groupby(["ACH_DATE"])["LIST_SIZE"].sum().reset_index()


# Used in Excel Table 2, age_gen_csv
def aggregate_list_size_by_age_and_gender(df: pd.DataFrame):

    return (
        df.groupby(["ACH_DATE", "AGE_BAND", "GENDER"])["LIST_SIZE"].sum().reset_index()
    )


# Used in Excel Table 3, reg_geo_csv
def aggregate_list_size_by_practice(df: pd.DataFrame) -> pd.DataFrame:

    return df.groupby(["ACH_DATE", "GP_PRACTICE"])["LIST_SIZE"].sum().reset_index()


# Used in Excel Table 4
def aggregate_list_size_by_sub_icb(df: pd.DataFrame) -> pd.DataFrame:

    return (
        df.groupby(["ACH_DATE", "SUB_ICB_LOCATION_CODE"])["LIST_SIZE"]
        .sum()
        .reset_index()
    )


def list_size_for_age_gen_csv(list_size_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates list size grouping by all required demographic measures for age_gen_csv.

    Args:
        list_size_df (pd.DataFrame): DataFrame containing list size disaggregated counts

    Returns:
        pd.DataFrame: A single concatenated List size dataframe, grouped by all required measure.
    """

    age_gen_list_size_measure_groups = [
        ["ACH_DATE"],
        ["ACH_DATE", "AGE_BAND", "GENDER"],
        ["ACH_DATE", "AGE_BAND"],
        ["ACH_DATE", "GENDER"],
    ]

    list_size_dfs = [
        aggregate_list_size_by_measure(list_size_df, measure)
        for measure in age_gen_list_size_measure_groups
    ]
    output = (
        pd.concat(list_size_dfs, ignore_index=True)
        .pipe(remap_age_band)
        .pipe(remap_gender_column)
    )
    output = output.fillna("All")

    return output


def list_size_for_reg_geog_csv(df: pd.DataFrame) -> pd.DataFrame:

    return (
        df.groupby(
            [
                "ACH_DATE",
                "GP_PRACTICE",
                "PRACTICE_NAME",
                "POSTCODE",
                "SUB_ICB_LOCATION_CODE",
                "ONS_SUB_ICB_LOCATION_CODE",
                "SUB_ICB_LOCATION_NAME",
                "ONS_ICB_CODE",
                "ICB_CODE",
                "ICB_NAME",
                "COMM_REGION_CODE",
                "ONS_COMM_REGION_CODE",
                "COMM_REGION_NAME",
            ]
        )["LIST_SIZE"]
        .sum()
        .reset_index()
    )
