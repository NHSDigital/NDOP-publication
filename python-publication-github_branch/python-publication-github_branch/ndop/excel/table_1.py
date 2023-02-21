import pandas as pd
import openpyxl
from ndop import utils
from ndop.excel import excel_utils
from ndop.config import config

from openpyxl.styles import Font

def aggregate_ndop_counts_by_month(
    df: pd.DataFrame, rename_column: str
) -> pd.DataFrame:
    """
    Aggregates NDOP counts by month. Renames summed count columns to rename_column.

    Args:
        df (pd.DataFrame): NDOP data for all months covering publication reporting period.
        rename_column (str): Name of summarised NDOP counts column.

    Returns:
        pd.DataFrame: Aggregated NDOP counts for each reporting month.
    """
    df = (
        df.groupby(["ACH_DATE"])["NHS_Number"]
        .count()
        .reset_index()
        .rename(columns={"NHS_Number": f"{rename_column}"})
    )

    return df


def aggregate_list_size_by_month(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(["ACH_DATE"])["LIST_SIZE"].sum().reset_index()


def insert_england_geography_codes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Temporary helper function to insert England Geography codes into Table 1.

    Args:
        df (pd.DataFrame): Table 1 DataFrame.

    Returns:
        pd.DataFrame: Table 1 DataFrame with England Geography codes inserted.
    """

    # Could get these from DSS CORP possibly?
    df["ONS code"] = "E92000001"
    df["Code"] = "Eng"
    df["Name"] = "England"

    return df


def rename_table_1_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Helper function to reorder and rename columns and sort dataframe by date.

    Args:
        df (pd.DataFrame): Table 1 dataframe.

    Returns:
        _type_: Table 1 dataframe, edited for publication.
    """
    df = df.rename(
        columns={"ACH_DATE": "Date", "OPT_OUT": "Opt-out", "LIST_SIZE": "List size"}
    ).reindex(
        columns=[
            "Date",
            "ONS code",
            "Code",
            "Name",
            "Opt-out",
            "List size",
            "Opt-out Rate",
            "Deceased",
        ]
    )
    return df


def table_1_header(report_date: config.reportDates) -> str:
    """
    Creates string description for Table 1 in Excel.

    Args:
        report_date (config.reportDates): report dates object with date end and date start attributes.

    Returns:
        str: Description of Table 1 with dates covering reporting periods.
    """
    start_month_year = pd.to_datetime(report_date.start_date).date().strftime("%B %Y")
    end_month_year = pd.to_datetime(report_date.end_date).date().strftime("%B %Y")

    return f"Table 1: Number of national data opt-outs, England, {start_month_year} - {end_month_year}"


def create_table_1_df(
    ndop_df: pd.DataFrame,
    ndop_deceased_df: pd.DataFrame,
    list_size: pd.DataFrame,
):

    ndop_aggregated = aggregate_ndop_counts_by_month(ndop_df, "OPT_OUT")
    ndop_deceased_aggregated = aggregate_ndop_counts_by_month(
        ndop_deceased_df, "Deceased"
    )
    list_size_aggregated = aggregate_list_size_by_month(list_size)

    df = (
        ndop_aggregated.merge(list_size_aggregated, on=["ACH_DATE"], how="left")
        .merge(ndop_deceased_aggregated, on=["ACH_DATE"], how="left")
        .pipe(utils.calculate_opt_out_rate)
        .pipe(insert_england_geography_codes)
        .sort_values(by=["ACH_DATE"], ascending=False)
        .pipe(utils.format_full_date)
        .pipe(rename_table_1_columns)
    )

    return df


def create_and_write_table_1(
    wb: openpyxl.Workbook,
    df: pd.DataFrame,
    report_date: config.reportDates,
) -> pd.DataFrame:

    wb = excel_utils.write_table_to_sheet(wb=wb, sheet_name="Table 1", table_data=df)
    ws = wb["Table 1"]
    
    # NHS Logo
    # excel_utils.add_nhs_logo_to_sheet(ws=ws)

    # Update header   
    ws["A9"] = table_1_header(report_date)

    # Source
    source_text =  """Source: NHS SPINE, NHS Digital\nOpen Exeter, NHAIS"""
    source_font =  Font(name='Arial', size = 9, bold=True)
    source_merge = 4
    excel_utils.write_single_val(ws, 'A', "<table1,source>", source_text, source_font, source_merge, alignment=True, row_height=24)

    # Notes title 
    notes = "Notes"
    notes_font = Font(name='Arial', size = 11, bold=True)
    excel_utils.write_single_val(ws, 'A', "<table1,notes>", notes, notes_font, merge=None,  alignment=True, row_height=15)

    # Note 1
    footer_1 = "1. The above data is correct at the time of publication, but these figures may change in subsequent publications."
    footer_1_font = Font(name='Arial', size = 10)
    excel_utils.write_single_val(ws, 'A', "<table1,footer1>", footer_1, footer_1_font, merge=None, alignment=False, row_height=20)

    # Note 2
    footer_2 = "2. We are aware of a slight inflation of less than 0.05 percent in the number of National Data Opt-outs. This is due to an issue with the data processing, which we are looking to resolve. This issue does not disproportionately affect any single breakdown, including geographies. Please take this into consideration when using the data."
    footer_2_font = Font(name='Arial', size = 10)
    footer_2_merge = 8
    excel_utils.write_single_val(ws, 'A', "<table1,footer2>", footer_2, footer_2_font, merge=footer_2_merge, alignment=True, row_height=40)

    # Copyright 
    copyright_text = 'Copyright © 2022, Health and Social Care Information Centre. The Health and Social Care Information Centre is a non-departmental body created by statute, also known as NHS Digital.'
    copyright_font = Font(name='Arial', size = 9)
    copyright_merge = 6
    excel_utils.write_single_val(ws, 'A', "<table1,copyright>", copyright_text, copyright_font, copyright_merge, alignment=True, row_height=37.5)

    return wb