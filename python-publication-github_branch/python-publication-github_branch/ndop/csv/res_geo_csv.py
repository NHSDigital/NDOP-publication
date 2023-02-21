import pandas as pd
import numpy as np

from ndop.preprocessing import ndop_aggregate


def join_lsoa_ndop_counts_with_geogs(
    ndop_df: pd.DataFrame, lsoa_df: pd.DataFrame
) -> pd.DataFrame:

    df = ndop_df.merge(lsoa_df, on="LSOA_CODE", how="left")

    return df


def replace_invalid_lsoa_with_unallocated(df: pd.DataFrame) -> pd.DataFrame:

    df["LSOA_CODE"] = np.where(df["LSOA_NAME"].isna(), "Unallocated", df["LSOA_CODE"])
    df = df.fillna("Unallocated")

    return df


def create_res_geo_csv(ndop_df: pd.DataFrame, lsoa_df: pd.DataFrame) -> pd.DataFrame:

    df = (
        ndop_df.pipe(ndop_aggregate.aggregate_ndop_by_lsoa)
        .merge(lsoa_df, on="LSOA_CODE", how="left")
        .pipe(replace_invalid_lsoa_with_unallocated)
        .groupby(
            [
                "ACH_DATE",
                "LSOA_CODE",
                "LSOA_NAME",
                "SUB_ICB_LOCATION_CODE",
                "ONS_SUB_ICB_LOCATION_CODE",
                "SUB_ICB_LOCATION_NAME",
                "LA_CODE",
                "LA_NAME",
            ]
        )["OPT_OUT"]
        .sum()
        .reset_index()
        .sort_values(by = ['ACH_DATE', 'LSOA_CODE'], ascending= [False, True])
    )

    return df
