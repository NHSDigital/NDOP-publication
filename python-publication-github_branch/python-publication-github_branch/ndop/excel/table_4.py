from typing import List, Set, Any
import pandas as pd
import numpy as np

from ndop.config import config, params
from ndop.preprocessing import list_size, ingest_icb, ndop_aggregate
from ndop import utils
from ndop.excel import excel_utils
from openpyxl.styles import Font

def preprocess_deceased_pats_for_table_4(df: pd.DataFrame, report_date: config.reportDates) -> pd.DataFrame: 
    """High level function: subsets deceased NDOP records to current month and aggregates by practice for table 4. 

    Args:
        df (pd.DataFrame): Concatenated NDOP deceased records.
        report_date (config.reportDates): report dates object

    Returns:
        pd.DataFrame: Count of deceased NDOP patient records for current month by practice. 
    """
    df = (
        df
        .pipe(filter_patients_for_current_month, report_date = report_date)
        .pipe(ndop_aggregate.aggregate_ndop_by_practice, rename_col= 'Deceased')
    )

    return df[['GP_PRACTICE', 'Deceased']]


def group_deceased_by_practice(df: pd.DataFrame) -> pd.DataFrame:

    return df.groupby(['GP_PRACTICE'])['OPT_OUT'].sum().reset_index().rename(columns = {'OPT_OUT': 'Deceased'}) 


def filter_patients_for_current_month(df: pd.DataFrame, report_date:config.reportDates) -> pd.DataFrame: 

    return df.query(f"ACH_DATE == '{report_date.end_date}'")


def map_unallocated_gp_practice(df: pd.DataFrame) -> pd.DataFrame: 

    df['GP_PRACTICE'] = np.where(df['SUB_ICB_LOCATION_CODE'].isna(), 'Unallocated', df['GP_PRACTICE'])

    return df


def fill_blank_geography_columns(df: pd.DataFrame) -> pd.DataFrame:

    columns = ['SUB_ICB_LOCATION_CODE', 'ONS_SUB_ICB_LOCATION_CODE', 'SUB_ICB_LOCATION_NAME']

    df[columns] = df[columns].fillna('Unallocated')

    return df


def group_ndop_list_size_by_sub_icb(df: pd.DataFrame) -> pd.DataFrame: 

    return df.groupby(['SUB_ICB_LOCATION_CODE', 'ONS_SUB_ICB_LOCATION_CODE', 'SUB_ICB_LOCATION_NAME']).agg({'OPT_OUT': 'sum', 'LIST_SIZE': 'sum', 'Deceased': 'sum'}).reset_index()



def rename_table_4_columns(df: pd.DataFrame) -> pd.DataFrame:

    df = df.rename(
        columns={
            "ONS_SUB_ICB_LOCATION_CODE": "ONS code",
            "SUB_ICB_LOCATION_CODE": "Code",
            "SUB_ICB_LOCATION_NAME": "Name",
            "LIST_SIZE": "List size",
            "OPT_OUT": "Opt-out",
        }
    )

    return df

def replace_empty_opt_out_rate_with_z(df: pd.DataFrame) -> pd.DataFrame: 

    df['Opt-out Rate'] = df['Opt-out Rate'].fillna('z')

    return df 

def create_table_4_df(
    reg_csv_df: pd.DataFrame, 
    ndop_deceased_df: pd.DataFrame,
    report_date: config.reportDates,
) -> pd.DataFrame:
    """High level function for creating data for table 4 of Excel summary. 
    Aggregates all processes all NDOP data for outputs. 

    Args:
        reg_csv_df (pd.DataFrame): Data from NDOP ccg of registration.
        ndop_deceased_df (pd.DataFrame): Data containing deceased patient NDOP records.
        report_date (config.reportDates): Report Date object. 

    Returns:
        pd.DataFrame: Cleaned data ready for Table 4 of Excel summary.
    """

    ndop_deceased_current_month_df = preprocess_deceased_pats_for_table_4(ndop_deceased_df, report_date)
    ndop_current_month_df = utils.filter_data_to_current_month(reg_csv_df, report_date)

    df = (
        ndop_current_month_df
        .merge(ndop_deceased_current_month_df, on = 'GP_PRACTICE', how = 'outer')
        .pipe(map_unallocated_gp_practice)
        .pipe(fill_blank_geography_columns)
        .pipe(group_ndop_list_size_by_sub_icb)
        .pipe(utils.calculate_opt_out_rate)
        .pipe(replace_empty_opt_out_rate_with_z)
        .pipe(rename_table_4_columns)
    )

    df = df[
        ["ONS code", "Code", "Name", "Opt-out", "List size", "Opt-out Rate", "Deceased"]
    ]

    return df

def table_4_header(report_date: config.reportDates) -> str:
    """
    Creates string description for Table 4 in Excel.

    Args:
        report_date (config.reportDates): report dates object with date end and date start attributes.

    Returns:
        str: Description of Table 4 with dates covering reporting periods.
    """
    end_month_year = pd.to_datetime(report_date.end_date).date().strftime("%B %Y")

    return f"Table 4: Number of national data opt-outs, Sub-ICB of Registration, {end_month_year}"


def create_and_write_table_4(wb, df: pd.DataFrame, report_date: config.reportDates) -> pd.DataFrame:

    wb = excel_utils.write_table_to_sheet(
        wb=wb,
        table_data=df,
        sheet_name="Table 4"
    )

    ws = wb["Table 4"]

    # NHS Logo
    # excel_utils.add_nhs_logo_to_sheet(ws=ws)

    # Update header
    ws["A9"] = table_4_header(report_date)

    # Add footers
    # TODO Wrap these parameters into a dictionary? 
    # Source
    source_text =  """Source: NHS SPINE, NHS Digital\nOpen Exeter, NHAIS"""
    source_font =  Font(name='Arial', size = 9, bold=True)
    source_merge = 3
    excel_utils.write_single_val(ws, 'A', "<table4,source>", source_text, source_font, source_merge, alignment=True, row_height=24)
     

    # Notes title 
    notes = "Notes"
    notes_font = Font(name='Arial', size = 11, bold=True)
    excel_utils.write_single_val(ws, 'A', "<table4,notes>", notes, notes_font, merge=None,  alignment=True, row_height=15)

    # Note 1
    footer_1 = '1. The Unallocated category above consists of individuals who either had no GP practice associated with their NHS number, or the GP practice associated with their NHS number is no longer open and active.'
    footer_1_font = Font(name='Arial', size = 10)
    footer_1_merge = 6
    excel_utils.write_single_val(ws, 'A', "<table4,footer1>", footer_1, footer_1_font, footer_1_merge, alignment=True, row_height=28)

    # Note 2
    footer_2 = 'Copyright © 2022, Health and Social Care Information Centre. The Health and Social Care Information Centre is a non-departmental body created by statute, also known as NHS Digital.'
    footer_2_font = Font(name='Arial', size = 9)
    footer_2_merge = 6
    excel_utils.write_single_val(ws, 'A', "<table4,footer2>", footer_2, footer_2_font, footer_2_merge, alignment=True, row_height=14)

    return wb
