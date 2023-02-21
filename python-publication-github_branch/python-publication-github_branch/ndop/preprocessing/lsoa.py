from pathlib import Path

import pandas as pd
from ndop.config import params, config


def get_lsoa_file_path():

    return Path(params.ROOT_DIR) / params.INPUTS_FOLDER / params.LSOA_FILE


def get_lsoa_mappings() -> pd.DataFrame:
    """
    Reads in LSOA-ICB-LA mappings from excel file and renames columns to match csv outputs.

    Returns:
        pd.DataFrame: DataFrame containing new ICB mappings
    """
    filepath = get_lsoa_file_path()

    df = pd.read_excel(filepath, params.LSOA_SHEET)

    # add renaming columns

    column_rename = {
        "LSOA11CD": "LSOA_CODE",
        "LSOA11NM": "LSOA_NAME",
        "LOC22CD": "ONS_SUB_ICB_LOCATION_CODE",
        "LOC22CDH": "SUB_ICB_LOCATION_CODE",
        "LOC22NM": "SUB_ICB_LOCATION_NAME",
        "ICB22CD": "ONS_ICB_CODE",
        "ICB22CDH": "ICB_CODE",
        "ICB22NM": "ICB_NAME",
        "LAD22CD": "LA_CODE",
        "LAD22NM": "LA_NAME",
    }

    df.rename(columns=column_rename, inplace=True)

    return df[
        [
            "LSOA_CODE",
            "LSOA_NAME",
            "ONS_SUB_ICB_LOCATION_CODE",
            "SUB_ICB_LOCATION_CODE",
            "SUB_ICB_LOCATION_NAME",
            "LA_CODE",
            "LA_NAME",
        ]
    ]


## This function should be used once LSOA table has been updated on dss_corp reference.
def preprocess_lsoa_df(report_date):

    connection = config.create_sql_connection(params.DSS_CORP_CONNECTION_STRING)
    data = config.get_lsoa_ccg_table(report_date, connection)

    return data

