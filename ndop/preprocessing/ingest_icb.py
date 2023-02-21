import pandas as pd
from pathlib import Path
import ndop.config.config as config
import ndop.config.params as params
import sqlalchemy

def corporate_reference_mapping(
    report_date: str, conn: sqlalchemy.engine.base.Engine
) -> pd.DataFrame:

    mapping_max_query = f"""
        SELECT DISTINCT a.DATE_OF_OPERATION, a.DH_GEOGRAPHY_CODE, a.DH_GEOGRAPHY_NAME, a.GEOGRAPHY_CODE
        FROM [DSS_CORPORATE].[dbo].[ONS_CHD_GEO_EQUIVALENTS] as a
        INNER JOIN (SELECT DH_GEOGRAPHY_CODE, MAX(DATE_OF_OPERATION) AS DATE_OF_OPERATION 
        FROM [DSS_CORPORATE].[dbo].[ONS_CHD_GEO_EQUIVALENTS] 
        WHERE DATE_OF_OPERATION <= '{report_date}' 
        GROUP BY DH_GEOGRAPHY_CODE) as b
        ON a.DATE_OF_OPERATION = b.DATE_OF_OPERATION
        AND a.DH_GEOGRAPHY_CODE = b.DH_GEOGRAPHY_CODE
        """

    mapping_max = pd.read_sql_query(mapping_max_query,conn)
    return mapping_max


def get_icb_mapping(report_date: str) -> pd.DataFrame:
    """
    Prepares icb mapping file use with other functions in publication.

    Args:
        report_date (config.reportDates): report_dates object with dates for publication.

    Returns:
        pd.DataFrame: ICB mapping from ONS_CHD_GEO_EQUIVALENTS.
    """

    connection = config.create_sql_connection(params.DSS_CORP_CONNECTION_STRING)
    icb_mapping_df = corporate_reference_mapping(report_date, connection)

    return icb_mapping_df
