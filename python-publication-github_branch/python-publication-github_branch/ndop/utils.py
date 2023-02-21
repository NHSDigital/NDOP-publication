from ndop.config import config, params
import pandas as pd
import os


def write_to_outputs_folder(
    df: pd.DataFrame, file_name: str, folder_name: str = "outputs"
) -> None:

    output_directory = os.path.join(params.ROOT_DIR, "outputs", f"{file_name}.csv")

    df.to_csv(output_directory, index=False)


def format_publication_date(df: pd.DataFrame) -> pd.DataFrame:

    """
    Transforms date column to dd-mm-yyyy format for publication outputs.

    Args:
        df(pd.DataFrame): DataFrame containing date column to be transformed.

    Returns:
        pd.DataFrame: DataFrame with correctly formatted date column.
    """
    df["ACH_DATE"] = pd.to_datetime(df["ACH_DATE"]).dt.strftime("%d/%m/%Y")

    return df


def format_full_date(df: pd.DataFrame) -> pd.DataFrame:

    df["ACH_DATE"] = pd.to_datetime(df["ACH_DATE"]).dt.strftime("%d %B %Y")

    return df


def filter_data_to_current_month(
    df: pd.DataFrame, report_date: config.reportDates
) -> pd.DataFrame:
    """
    Function for filtering data in report month.

    Args:
        df (pd.DataFrame): DataFrame containing 'ACH_DATE' column.
        report_date (config.reportDates): Date object.

    Returns:
        pd.DataFrame: Filtered data for current reporting month.
    """
    return df[
        df["ACH_DATE"]
        == pd.to_datetime(report_date.end_date).date().strftime("%d/%m/%Y")
    ]


def calculate_opt_out_rate(df: pd.DataFrame) -> pd.DataFrame:
    df["Opt-out Rate"] = 100 * (df["OPT_OUT"] / df["LIST_SIZE"])

    return df
