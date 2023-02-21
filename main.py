from ndop.config import config, params
from ndop.excel.table_1 import create_table_1_df
from ndop.excel.table_2 import create_table_2_df
from ndop.excel.table_3 import create_table_3_df
from ndop.excel.table_4 import create_table_4_df
from ndop.preprocessing import ndop_aggregate, list_size, lsoa
from ndop.csv import age_gen_csv, reg_geog_csv, res_geo_csv
from ndop.excel import create_excel
from ndop import utils
import argparse


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='NDOP monthly publication')
    parser.add_argument('--RPED', type=str, required=True, help='date of publication')
    parser.add_argument('--pub_date', type=str, required=True, help='date of publication')
    parser.add_argument('--months', nargs='?', const=1, type=int, default=11)
    args = parser.parse_args()

    report_dates = config.reportDates(args.RPED, publishing_date = args.pub_date, reporting_period=args.months)

    list_size_df = list_size.preprocess_list_size(report_dates)
    lsoa_df = lsoa.get_lsoa_mappings()
    print("List size fetched")

    # NDOP data needs to be separated into active records and deceased patients
    ndop_concat_df, ndop_deceased_concat_df = ndop_aggregate.preprocess_ndop_data(
        report_dates
    )
    print("NDOP preprocessing complete")

    # Create DataFrames which will populate Excel and CSV outputs
    ndop_age_gen_csv = age_gen_csv.create_age_gen_csv(
        ndop_concat_df,
        ndop_deceased_concat_df,
        list_size_df,
    )
    print("Age_gen csv created")

    ndop_reg_geo_csv = reg_geog_csv.create_reg_geo_csv(ndop_concat_df, list_size_df)
    print("Reg_geo csv created")
    ndop_res_geo_csv = res_geo_csv.create_res_geo_csv(ndop_concat_df, lsoa_df)
    print("Res_geo csv created")

    # Create Excel Sheets
    table_1_df = create_table_1_df(
        ndop_concat_df, ndop_deceased_concat_df, list_size_df
    )
    table_2_df = create_table_2_df(ndop_age_gen_csv, report_dates)
    table_3_df = create_table_3_df(ndop_reg_geo_csv, report_dates)
    table_4_df = create_table_4_df(
        ndop_reg_geo_csv, ndop_deceased_concat_df, report_dates
    )

    # Creating Excel
    create_excel.create_excel_publication(
        table_1_df, table_2_df, table_3_df, table_4_df, report_dates
    )
    print("Excel file created")
    # # Write to outputs folder

    utils.write_to_outputs_folder(
        ndop_age_gen_csv, f"NDOP_age_gen_{report_dates.month_year_date_format()}"
    )
    utils.write_to_outputs_folder(
        ndop_reg_geo_csv, f"NDOP_reg_geog_{report_dates.month_year_date_format()}"
    )
    utils.write_to_outputs_folder(
        ndop_res_geo_csv, f"NDOP_res_geog_{report_dates.month_year_date_format()}"
    )
    print("All files written")
