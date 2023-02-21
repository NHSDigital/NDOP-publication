import pandas as pd
import numpy as np
from ndop import utils
from ndop.config import config
from ndop.excel import excel_utils

from openpyxl.styles import Font

def reorder_practices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates different practice cohorts to match Excel Table 3 output and concatentate into single dataframe.
    Rearranges practices in the order: practices with opt outs, practices with 0 opt outs and unallocated practice opt outs.

    Args:
        df (pd.DataFrame): Data for Table 3.

    Returns:
        pd.DataFrame: Data for Table 3 reorganised in correct format.
    """
    practice_standard = df[(df["OPT_OUT"] > 0) & (df["PRACTICE_NAME"] != "Unallocated")].copy()

    practice_zero_opt_out = df[df["OPT_OUT"] == 0].copy()
    practice_zero_opt_out["OPT_OUT"] = practice_zero_opt_out["OPT_OUT"].replace(0, "-")

    practice_unallocated = df[df["PRACTICE_NAME"] == "Unallocated"].copy()
    practice_unallocated["Opt-out Rate"] = practice_unallocated["Opt-out Rate"].replace(
        np.inf, "z"
    )

    return pd.concat([practice_standard, practice_zero_opt_out, practice_unallocated])


def table_3_header(report_date: config.reportDates) -> str:
    """
    Creates string description for Table 3 in Excel.

    Args:
        report_date (config.reportDates): report dates object with date end and date start attributes.

    Returns:
        str: Description of Table 3 with dates covering reporting periods.
    """
    end_month_year = pd.to_datetime(report_date.end_date).date().strftime("%B %Y")

    return f"Table 3: Number of national data opt-outs, GP Practice, {end_month_year}"


def create_table_3_df(
    df: pd.DataFrame, report_date: config.reportDates
) -> pd.DataFrame:

    df = (
        df.pipe(utils.filter_data_to_current_month, report_date=report_date)
        .pipe(utils.calculate_opt_out_rate)
        .sort_values(by=["GP_PRACTICE"])
        .pipe(reorder_practices)
        .rename(
            columns={
                "GP_PRACTICE": "Practice code",
                "PRACTICE_NAME": "Practice name",
                "OPT_OUT": "Opt-out",
                "LIST_SIZE": "List size",
            }
        )
        .reset_index()
    )

    df = df[["Practice code", "Practice name", "Opt-out", "List size", "Opt-out Rate"]]

    return df


def create_and_write_table_3(wb, df: pd.DataFrame, report_date:config.reportDates) -> pd.DataFrame:

    wb = excel_utils.write_table_to_sheet(
        wb=wb,
        table_data=df,
        sheet_name="Table 3",
    )

    ws = wb["Table 3"]

    # NHS Logo
    # excel_utils.add_nhs_logo_to_sheet(ws=ws)

    # Update header
    ws["A9"] = table_3_header(report_date)

    # Source
    source_text =  """Source: NHS SPINE, NHS Digital\nOpen Exeter, NHAIS"""
    source_font =  Font(name='Arial', size = 9, bold=True)
    source_merge = 3
    excel_utils.write_single_val(ws, 'A', "<table3,source>", source_text, source_font, source_merge, alignment=True, row_height=24)

    # Notes title 
    notes = "Notes"
    notes_font = Font(name='Arial', size = 11, bold=True)
    excel_utils.write_single_val(ws, 'A', "<table3,notes>", notes, notes_font, merge=None,  alignment=True, row_height=15)

    # Note 1
    footer_1 = '1. The above data is correct at the time of publication, but these figures may change in subsequent publications.'
    footer_1_font = Font(name='Arial', size = 10)
    excel_utils.write_single_val(ws, 'A', "<table3,footer1>", footer_1, footer_1_font, merge=None, alignment=False, row_height=17)

    # Note 2
    footer_2 = '''2. We are aware of a slight inflation of less than 0.05 percent in the number of National Data Opt-outs. This is due to an issue with the data processing, which we are looking to resolve. This issue does not disproportionately affect any single breakdown, including geographies. Please take this into consideration when using the data.'''
    footer_2_font = Font(name='Arial', size = 10)
    footer_2_merge = 8
    excel_utils.write_single_val(ws, 'A', "<table3,footer2>", footer_2, footer_2_font, footer_2_merge, alignment=True, row_height=30)

    # Note 3
    footer_3 = '''3. The increase in October and November 2020 could be attributed to a number of posts circulated on social media about the national data opt-out, containing incorrect information. This page looks at these claims and gives you the facts you need to make your choice: https://digital.nhs.uk/services/national-data-opt-out/mythbusting-social-media-posts'''
    footer_3_font = Font(name='Arial', size = 10)
    footer_3_merge = 8
    excel_utils.write_single_val(ws, 'A', "<table3,footer3>", footer_3, footer_3_font, footer_3_merge, alignment=True, row_height=39.5)
    
    # Copyright 
    copyright_text = 'Copyright © 2022, Health and Social Care Information Centre. The Health and Social Care Information Centre is a non-departmental body created by statute, also known as NHS Digital.'
    copyright_font = Font(name='Arial', size = 9)
    copyright_merge = 6
    excel_utils.write_single_val(ws, 'A', "<table3,copyright>", copyright_text, copyright_font, copyright_merge, alignment=True, row_height=23)

    return wb
