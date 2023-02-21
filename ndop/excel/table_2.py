import pandas as pd
from pandas.api.types import CategoricalDtype
from ndop import utils
from ndop.config import config
from ndop.excel import excel_utils

from openpyxl.styles import Font


def remap_table_2_age_band(df: pd.DataFrame) -> pd.DataFrame:
    """
    Formats Age bands to match output spec for Table 2.

    Args:
        df (pd.DataFrame): DataFrame containing data for Table 2.

    Returns:
        pd.DataFrame: DataFrame with correctly formatted age bands.
    """
    df["AGE_BAND"] = df["AGE_BAND"].str.replace("-", " to ")
    df["AGE_BAND"] = df["AGE_BAND"].str.replace("+", " +", regex=False)
    df["AGE_BAND"] = df["AGE_BAND"].str.replace("All deceased", "Deceased")

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
            "0 to 9",
            "10 to 19",
            "20 to 29",
            "30 to 39",
            "40 to 49",
            "50 to 59",
            "60 to 69",
            "70 to 79",
            "80 to 89",
            "90 +",
            "All",
            "Deceased",
        ],
        ordered=True,
    )

    df["AGE_BAND"] = df["AGE_BAND"].astype(age_band_column_orders)

    return df


def convert_gender_column_to_categorical(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts GENDER column in dataframe to categorical type to enable custom sort.

    Args:
        df (pd.DataFrame): NDOP aggregated counts containing GENDER groups.

    Returns:
        pd.DataFrame: NDOP dataframe with GENDER group converted to categorical type.
    """
    gender_column_order = CategoricalDtype(
        ["Female", "Male", "Unknown / Prefer not to say"],
        ordered=True,
    )

    df["GENDER"] = df["GENDER"].astype(gender_column_order)

    return df


def remove_total_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes total counts for certain demographics to match spec for table 2.

    Args:
        df (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: Correct rows for Table 2.
    """
    df = df[~df["GENDER"].isin(["All", "All deceased"])]
    df = df.drop(
        df[(df["AGE_BAND"] == "All") & (df["GENDER"].isin(["Male", "Female"]))].index
    )

    return df


def move_deceased_groups_to_end(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function for reordering the Table 2 outputs so Deceased patient counts are arranged at the bottom of dataframe.
    This must come after columns are renamed to 'Age' or it will raise an error.

    Args:
        df (pd.DataFrame): DataFrame for Table 2 data.

    Returns:
        pd.DataFrame: Table 2 DataFrame in correct order for output.
    """

    df_living = df[df["Age"] != "Deceased"]
    df_deceased = df[df["Age"] == "Deceased"]

    return pd.concat([df_living, df_deceased])


def create_table_2_df(
    df: pd.DataFrame, report_date: config.reportDates
) -> pd.DataFrame:

    df = (
        df.pipe(utils.filter_data_to_current_month, report_date=report_date)
        .pipe(remove_total_counts)
        .pipe(remap_table_2_age_band)
        .pipe(convert_age_column_to_categorical)
        .pipe(convert_gender_column_to_categorical)
        .rename(
            columns={
                "AGE_BAND": "Age",
                "GENDER": "Gender",
                "OPT_OUT": "Opt-out",
                "LIST_SIZE": "List size",
                "OPT_OUT_RATE": "Opt-out Rate",
            }
        )
        .sort_values(by=["Gender", "Age"])
        .pipe(move_deceased_groups_to_end)
        .replace(0, "z")
        .reset_index()
    )

    df = df[["Age", "Gender", "Opt-out", "List size", "Opt-out Rate"]]

    # Note: openpyxl seems to have an issue with CategoricalDType so these columsn are reverted
    # to string after sorting.
    df["Age"] = df["Age"].astype(str)
    df["Gender"] = df["Gender"].astype(str)

    return df



def table_2_header(report_date: config.reportDates) -> str:
    """
    Creates string description for Table 2 in Excel.

    Args:
        report_date (config.reportDates): report dates object with date end and date start attributes.

    Returns:
        str: Description of Table 2 with dates covering reporting periods.
    """
    end_month_year = pd.to_datetime(report_date.end_date).date().strftime("%B %Y")

    return f"Table 2: Number of national data opt-outs, by Age and Gender, {end_month_year}"


def create_and_write_table_2(wb, df: pd.DataFrame, report_date:config.reportDates) -> pd.DataFrame:

    wb = excel_utils.write_table_to_sheet(
        wb=wb,
        table_data=df,
        sheet_name="Table 2",
    )

    ws = wb["Table 2"]

     # NHS Logo
    # excel_utils.add_nhs_logo_to_sheet(ws=ws)
    
    # Update header
    ws["A9"] = table_2_header(report_date)

    # Source
    source_text =  """Source: NHS SPINE, NHS Digital\nOpen Exeter, NHAIS"""
    source_font =  Font(name='Arial', size = 9, bold=True)
    source_merge = 3
    excel_utils.write_single_val(ws, 'A', "<table2,source>", source_text, source_font, source_merge, alignment=True, row_height=24)

    # Notes title 
    notes = "Notes"
    notes_font = Font(name='Arial', size = 11, bold=True)
    excel_utils.write_single_val(ws, 'A', "<table2,notes>", notes, notes_font, merge=None,  alignment=True, row_height=15)

    # Note 1
    footer_1 = "1.The unknown category above includes those records which have been closed and gender is not included, or those who have chosen to 'prefer not to say' when asked their gender."
    footer_1_font = Font(name='Arial', size = 10)
    footer_1_merge = 5
    excel_utils.write_single_val(ws, 'A', "<table2,footer1>", footer_1, footer_1_font, merge=footer_1_merge, alignment=True, row_height=37)

    # Copyright 
    copyright_text = 'Copyright © 2022, Health and Social Care Information Centre. The Health and Social Care Information Centre is a non-departmental body created by statute, also known as NHS Digital.'
    copyright_font = Font(name='Arial', size = 9)
    copyright_merge = 5
    excel_utils.write_single_val(ws, 'A', "<table2,copyright>", copyright_text, copyright_font, copyright_merge, alignment=True, row_height=24.5)

    return wb
