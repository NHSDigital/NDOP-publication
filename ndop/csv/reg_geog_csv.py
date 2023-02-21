import pandas as pd
import numpy as np
from ndop.preprocessing import ndop_aggregate, list_size, ingest_icb
from ndop import utils


def create_reg_geo_csv(
    ndop_df: pd.DataFrame, list_size_df: pd.DataFrame
) -> pd.DataFrame:
    """
    High level function which prepares and joins NDOP data with list size ready for creating reg_geog_csv.

    Args:
        ndop_df (pd.DataFrame): Cleaned NDOP dataframe.
        list_size_df (pd.DataFrame): Cleaned List size data ready for reg_geog_csv.

    Returns:
        pd.DataFrame: NDOP data summarised and prepared for export to reg_geog_csv.
    """

    df = (
        ndop_df
        .pipe(merge_registered_ndop_with_list_size, list_size_df=list_size_df)
        .pipe(process_records_with_empty_postcode)
        .pipe(groupby_all_categorical_columns)
        .pipe(utils.format_publication_date)
    )

    return df


def merge_registered_ndop_with_list_size(
    ndop_df: pd.DataFrame, list_size_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Combine NDOP, List Size and ICB Mapping details into single dataframe.
    Results with empty Opt-out count or list size are filled with 0.

    Args:
        ndop_df (pd.DataFrame): cleaned NDOP data.
        list_size_df (pd.DataFrame): List Size data grouped by practice code.

    Returns:
        pd.DataFrame: Merged data for reg_geog csv.
    """

    ndop_by_practice_df = ndop_aggregate.aggregate_ndop_by_practice(ndop_df)
    list_size_by_practice_df = list_size.list_size_for_reg_geog_csv(list_size_df)
    
    merged_df = ndop_by_practice_df.merge(
        list_size_by_practice_df, on=["ACH_DATE", "GP_PRACTICE"], how="outer"
    )

    merged_df["OPT_OUT"] = merged_df["OPT_OUT"].fillna(0).astype(int)
    merged_df["LIST_SIZE"] = merged_df["LIST_SIZE"].fillna(0).astype(int)

    return merged_df


def process_records_with_empty_postcode(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes and fills records with empty postcodes.

    Following merge of NDOP and list size, practices with empty postcodes arise as a result of not having an active list size for the ACH_DATE.
    GP Practice column of these practices are replaced with 'Unallocated'. The empty categorical column values are then filled with 'Unallocated'.

    Args:
        df (pd.DataFrame): NDOP and list sized merged dataframe.

    Returns:
        pd.DataFrame: NDOP data with 'Unallocated' practice recordss.
    """

    empty_postcode = df["POSTCODE"].isna()

    df["GP_PRACTICE"] = np.where(empty_postcode, "Unallocated", df["GP_PRACTICE"])
    df["LIST_SIZE"] = np.where(empty_postcode, 0, df["LIST_SIZE"])

    df = df.fillna("Unallocated")

    return df


def groupby_all_categorical_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates NDOP counts across all categorical columns.
    This is included to sum the NDOP counts across the newly created 'Unallocated' GP practice records.

    Args:
        df (pd.DataFrame): NDOP dataframe with Unallocated records included.

    Returns:
        pd.DataFrame: NDOP DataFrame ready for export.
    """
    df = (
        df.groupby(
            [
                "ACH_DATE",
                "GP_PRACTICE",
                "POSTCODE",
                "PRACTICE_NAME",
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
        )
        .agg({"OPT_OUT": "sum", "LIST_SIZE": "sum"})
        .reset_index()
        .sort_values(by=["ACH_DATE", "GP_PRACTICE"], ascending=[False, True])
    )

    return df
