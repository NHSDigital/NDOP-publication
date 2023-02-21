import os
import pathlib
from pandas.api.types import CategoricalDtype

# These settings are for reading in ICB mappings if still needed
ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", ".."))

INPUTS_FOLDER = "inputs"
OUTPUTS_FOLDER = "outputs"
TEMPLATES_FOLDER = "templates"


ICB_FILE = "CCG_ICB_mapping_July_2022.xlsx"
ICB_SHEET = "LOC22_ICB22_NHSER22_EN_LU"

LSOA_FILE = "LSOA11_LOC22_ICB22_LAD22_EN_LU.xlsx"
LSOA_SHEET = "LSOA11_LOC22_ICB22_LAD22"

NHS_LOGO_IMG = 'nhs_logo.png'

EXCEL_TEMPLATE = "NDOP_sum_template.xlsx"

#connection strings have been redacted

NDOP_CONNECTION_STRING = (
    #######
)
DSS_CORP_CONNECTION_STRING = #######

INVALID_NHS_NUMBERS = [
    "1111111111",
    "2222222222",
    "3333333333",
    "4444444444",
    "5555555555",
    "6666666666",
    "7777777777",
    "8888888888",
    "9999999999",
]
