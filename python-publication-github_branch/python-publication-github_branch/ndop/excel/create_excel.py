from ndop.excel import excel_utils, table_1, table_2, table_3, table_4
import os
import openpyxl
from ndop.config import params


def create_excel_publication(table_1_df, table_2_df, table_3_df, table_4_df, report_date):

    # Set up
    output_path = os.path.join(params.ROOT_DIR, params.OUTPUTS_FOLDER, f"NDOP_sum_{report_date.month_year_date_format()}.xlsx")

    # Writing data to template
    wb = openpyxl.load_workbook(excel_utils.get_excel_template())
    wb['Title sheet']['A8'] = f'Date Published: {report_date.format_publishing_date()}'

    wb = table_1.create_and_write_table_1(wb, table_1_df, report_date)
    wb = table_2.create_and_write_table_2(wb, table_2_df, report_date)
    wb = table_3.create_and_write_table_3(wb, table_3_df, report_date)
    wb = table_4.create_and_write_table_4(wb, table_4_df, report_date)

    wb.save(output_path)
