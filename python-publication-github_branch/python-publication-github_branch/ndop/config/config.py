from dataclasses import dataclass, field
from typing import List
import pandas as pd
import datetime
import sqlalchemy
from sqlalchemy.engine import URL
from ndop.config import params
from pathlib import Path


@dataclass
class reportDates:
    """
    A class that creates a date object to handle report dates.

    Parameters:
        end_date: report period end date for publication.
        reporting_period: number of months publication covers.
        start_date: report period start date of publication.
        publishing_date: date that report will be published.

    Returns:
        reportDates: A report date object containing start date and end date.
    """

    end_date: str
    publishing_date: str
    reporting_period: int = 11
    start_date: str = field(init=False)

    def __post_init__(self):

        self.start_date = self.get_report_period_start_date()

    def get_report_period_start_date(self) -> str:

        """
        Calculates report start date based on number of months in the reporting period.

        Returns:
            str: Report period start date.
        """

        report_date = pd.to_datetime(self.end_date)
        report_start_date = report_date - pd.DateOffset(months=self.reporting_period)

        return report_start_date.date().strftime("%Y-%m-%d")

    def format_publishing_date(self) -> str:

        """
        Produces publishing date into correct format for publication.

        Returns:
            str: Report publication date.
        """

        date = pd.to_datetime(self.publishing_date).day

        suffix = ""

        if 4 <= date <= 20 or 24 <= date <= 30:
            suffix = "th"
        else:
            suffix = ["st", "nd", "rd"][date % 10 - 1]

        return pd.to_datetime(self.publishing_date).date().strftime(f"%d{suffix} %B %Y")

    def get_reporting_months_list(self) -> List[str]:

        return (
            pd.date_range(self.start_date, self.end_date, freq="MS")
            .strftime("%Y-%m-%d")
            .tolist()
        )

    def month_year_date_format(self):

        report_date = pd.to_datetime(self.end_date)

        return report_date.date().strftime("%b_%Y")


def create_sql_connection(connection_details: str) -> sqlalchemy.engine.base.Engine:
    """
    Creates a connection for connection to sql database.

    Args:
        connection_details (str): string representation for sql connection.
        E.g. 'Driver={driver_detail}; Server=server_detail, Database=Database; Trusted_Connection=yes;'

    Returns:
        sqlalchemy.engine.base.Engine: SQL connection for use with pandas read_sql command.
    """

    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": connection_details}
    )

    return sqlalchemy.create_engine(connection_url)


def get_ndop_version_log(connection: sqlalchemy.engine.base.Engine) -> pd.DataFrame:

    query = "SELECT * FROM PRIM_POS.ic.NDOP_Q1_CONFIG"

    return pd.read_sql(query, connection)


def check_ndop_version_is_current(df: pd.DataFrame, version_check: str = True) -> None:

    """
    Checks NDOP table version is same as current date, assertion error raised if not.

    Args:
        df(pd.DataFrame) - dataframe containing NDOP version.
        version_check(str) = skips version check if set to False.

    Returns:
        None

    """

    if version_check == False:
        print("Skipping Version Check Skipped.")

    else:
        max_current_date = max(pd.to_datetime(df["VERSION_FROM_DATETIME"]).dt.date)
        today_date = datetime.date.today()
        try:
            assert max_current_date == today_date
            print("NDOP version is current.")
        except AssertionError:
            print("Version is not current.")


def get_ndop_data(
    report_date: reportDates, connection: sqlalchemy.engine.base.Engine
) -> pd.DataFrame:

    query = f"""
            SELECT NHS_Number, 
            AGE_BAND, 
            CAST(DATE_OF_DEATH AS DATE) AS DATE_OF_DEATH, 
            GP_PRACTICE, 
            --REGISTERED_CCG, 
            --CCG_OF_RESIDENCE, 
            --LOCAL_AUTHORITY,
            LSOA AS LSOA_CODE, 
            GENDER, 
            CAST(Record_Start_Date AS DATE) AS Record_Start_Date, 
            CAST(Record_End_Date AS DATE) AS Record_End_Date  
            FROM PRIM_POS.ic.NDOP_DEMOG
            WHERE (CAST(Record_Start_Date AS DATE) <= '{report_date.end_date}')
            AND (CAST(Record_End_Date AS DATE) >= '{report_date.start_date}' OR CAST(Record_End_Date AS DATE) IS NULL)
            """

    return pd.read_sql(query, connection)


def get_list_size_data(
    report_dates: reportDates, conn: sqlalchemy.engine.base.Engine
) -> pd.DataFrame:
    """
    Returns list size data from SQL database between report end date and report start dates.

    Args:
        report_dates(reportDates): Date Object, function uses end_date and start_date attributes to filter records.
        conn(sqlalchemy.engine.base.Engine): connection object to access SQL database.

    Returns:
        pd.DataFrame: List size raw data from SQL.
    """
    query = f"""
            SELECT *,
            PRACTICE_CODE AS GP_PRACTICE,
            EXTRACT_DATE AS ACH_DATE 
            FROM dbo.GP_PATIENT_LIST 
            WHERE EXTRACT_DATE BETWEEN '{report_dates.start_date}' AND '{report_dates.end_date}'
            """

    return pd.read_sql_query(query, conn)

def get_geography_names(conn: sqlalchemy.engine.base.Engine) -> pd.DataFrame:

    query = f"""
            SELECT DH_GEOGRAPHY_CODE, 
            DH_GEOGRAPHY_NAME, 
            GEOGRAPHY_CODE 
            FROM [DSS_CORPORATE].dbo.['ONS_CHD_GEOG_EQUIVALENT]
            WHERE ENTITY_CODE IS IN ('E38', 'E54', 'E40')
            AND IS_CURRENT = 1
            """

    df = pd.read_sql(query, conn)

    return df

# This function is not used until ref table is updated.
def get_lsoa_ccg_table(
    report_date: reportDates, conn: sqlalchemy.engine.base.Engine
) -> pd.DataFrame:

    query = f"""
            SELECT
            LSOACD AS LSOA_CODE,
            LSOANM AS LSOA_NAME, 
            CCGCDH AS SUB_ICB_LOCATION_CODE, 
            --CCGCD AS ONS_SUB_ICB_LOCATION_CODE, 
            --CCGNM AS SUB_ICB_LOCATION_NAME, 
            LADCD AS LA_CODE,
            LADNM AS LA_NAME
            FROM DSS_CORPORATE.dbo.ONS_LSOA_CCG_STP_LAD_V01 
            WHERE [DSS_RECORD_START_DATE] <= '{report_date.end_date}' 
            AND ([DSS_RECORD_END_DATE] >= '{report_date.end_date}'
            OR [DSS_RECORD_END_DATE] IS NULL)"""

    return pd.read_sql_query(query, conn)


def get_categorical_columns() -> dict[str:str]:

    dict = {
        "GP_PRACTICE": "Unallocated",
        "REGISTERED_CCG": "Unallocated",
        "CCG_OF_RESIDENCE": "Unallocated",
        "LOCAL_AUTHORITY": "Unallocated",
        "LSOA": "Unallocated",
        "GENDER": "Unknown / Prefer not to say",
    }

    return dict


def create_age_cols():
    age_index_cols = [
        "ORIGINAL",
        "SEX",
        "AGE",
        "AGE_ONE_YEAR",
        "AGE_FIVE_YEAR",
        "AGE_TEN_YEAR",
    ]

    ## Creates Age_Index dataframe used throughout
    ## This means we can merge on Patient_data_mapped to get different age band columns
    Age_Index = pd.DataFrame(columns=age_index_cols)

    ## Creates ORIGINAL columns: "FEMALE_0_1","FEMALE_1_2" up to 119_120
    orig_cols_m = ["MALE_" + str(i) + "_" + str(i + 1) for i in range(0, 120)]
    orig_cols_f = ["FEMALE_" + str(i) + "_" + str(i + 1) for i in range(0, 120)]
    orig_cols = orig_cols_m + orig_cols_f
    Age_Index["ORIGINAL"] = orig_cols

    ## Creates AGE_***_YEAR columns:
    ## AGE_ONE_YEAR: "MALE_0_1","MALE_1_2" up to "MALE_95+"
    ## AGE_FIVE_YEAR: "MALE_0_4", "MALE_5_9" up to "MALE_95+"
    ## AGE_TEN_YEAR: "MALE_0_9", "MALE_10_19" up to "MALE_90+"
    ## Does both MALE and FEMALE columns, then adds them to Age_Index dataframe

    col_list = age_index_cols[-3:]
    sex_list = ["MALE_", "FEMALE_"]

    # gap_list is a list of tuples with the format below
    # (gap-1, range start, range stop, range step, string of + category, times the + category occurs)

    gap_list = [
        (1, 0, 95, 1, "95+", 25),
        (4, 0, 95, 5, "95+", 25),
        (9, 0, 90, 10, "90+", 30),
    ]
    one_five_ten_list = []

    for gap in gap_list:
        insertlist = []
        for sex in sex_list:
            single_cols = [
                sex + str(i) + "_" + str(i + gap[0])
                for i in range(gap[1], gap[2], gap[3])
            ]
            multi_cols = []
            for row in single_cols:
                gap_cols = [row] * gap[3]
                multi_cols.extend(gap_cols)
            plus_col = [sex + gap[4]] * gap[5]
            multi_cols.extend(plus_col)
            insertlist.extend(multi_cols)
        one_five_ten_list.append(insertlist)

    for col, val in zip(col_list, one_five_ten_list):
        Age_Index[col] = val

    ## Creates SEX column: "MALE" or "FEMALE" based on original column split
    Age_Index["SEX"] = Age_Index["AGE_ONE_YEAR"].str.split("_", expand=True).iloc[:, 0]

    ## Creates AGE column: Single year e.g. MALE_14_15 = 14 up to MALE_95+ = 95+
    Age_Index["AGE"] = ([*range(0, 95)] + ["95+"] * 25) * 2

    Age_Index = Age_Index.filter(["ORIGINAL", "SEX", "AGE_TEN_YEAR"]).rename(
        columns={"ORIGINAL": "AGE_GEN", "SEX": "GENDER"}
    )

    Age_Index["AGE_BAND"] = Age_Index["AGE_TEN_YEAR"].str.strip("MALE_|FEMALE_")

    return Age_Index
