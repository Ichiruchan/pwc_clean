from loguru import logger
import pandas as pd
from itertools import product

from py.analyze_data import get_column_from_df, check_col_type, check_col_range, check_col_list, check_col_conflict
from py.clean_data import reset_cols_df, reset_cols_type_df, drop_wrong_index_df
from py.extract_data import get_ddf_from_excel
from py.to_db import origin_2_db
from config.data_config import col_dict
from py.comparison import get_wrong_data_db_from_origin, get_wrong_data_origin_from_db

if __name__ == "__main__":
    dict_df = get_ddf_from_excel(file_path="data/Example_Data.xlsx",
                                 sheet_name=["Example_Data", "Example_DB"])
    data_origin_0 = dict_df["Example_Data"]
    data_db_0 = dict_df["Example_DB"]
    data_origin = reset_cols_df(df=data_origin_0,
                                new_columns=0).reset_index()
    print("Origin data columns are {cols}".format(cols=', '.join(get_column_from_df(df=data_origin))))
    data_origin_filled = data_origin.dropna()
    nan_index = list(set(data_origin.index).difference(set(data_origin_filled.index)))
    logger.info(
        "Because of unfilled value, drop these index {index}".format(index=", ".join(list(map(str, nan_index)))))
    check_cols_type = {"Company ID": str,
                       "Company Name": str,
                       "Fiscal Year": int,
                       "Industry": str,
                       "SIC Code": int,
                       "Trading Currency": str,
                       "SP": int,
                       "CDS": int,
                       "APD": int,
                       "ARD": int,
                       "ADA": int}
    wrong_type_index = list()
    for col, check_type in check_cols_type.items():
        check_res = check_col_type(col_values=data_origin_filled[col],
                                   value_type=check_type)
        col_wrong_index = list(check_res[check_res == False].index)
        wrong_type_index += col_wrong_index
        if len(col_wrong_index) > 0:
            logger.info(f"Column {col} has wrong type value at index {col_wrong_index}")
    wrong_type_index = list(set(wrong_type_index))
    logger.info("Origin data has wrong type index "
                "{wrong_index}".format(wrong_index=", ".join(list(map(str, wrong_type_index)))))
    data_origin_type_correct = drop_wrong_index_df(df=data_origin_filled,
                                                   wrong_index_list=wrong_type_index)
    data_origin_type_correct = reset_cols_type_df(df=data_origin_type_correct,
                                                  cols_type={"Company ID": str,
                                                             "Company Name": str,
                                                             "Fiscal Year": int,
                                                             "Industry": str,
                                                             "SIC Code": int,
                                                             "Trading Currency": str,
                                                             "SP": int,
                                                             "CDS": int,
                                                             "APD": int,
                                                             "ARD": int,
                                                             "ADA": int}
                                                  )

    check_cols_range = {"Fiscal Year": [1999, 2021],
                        "SIC Code": [1000, 9999]}
    wrong_range_index = list()
    for col, col_range in check_cols_range.items():
        check_res = check_col_range(col_values=data_origin_type_correct[col],
                                    min_num=col_range[0],
                                    max_num=col_range[1])
        col_wrong_index = list(check_res[check_res == False].index)
        wrong_range_index += col_wrong_index
        if len(col_wrong_index) > 0:
            logger.info(f"Column {col} has wrong range value at index {col_wrong_index}")
    wrong_range_index = list(set(wrong_range_index))
    logger.info("Origin data has wrong range index "
                "{wrong_index}".format(wrong_index=", ".join(list(map(str, wrong_range_index)))))

    wrong_list_index = list()
    check_cols_list = {"Trading Currency": ["USD", "GBP"]}
    for col, check_cols_list in check_cols_list.items():
        check_res = check_col_list(col_values=data_origin_type_correct[col],
                                   value_list=check_cols_list)
        col_wrong_index = list(check_res[check_res == False].index)
        wrong_list_index += col_wrong_index
        if len(col_wrong_index) > 0:
            logger.info(f"Column {col} has wrong list value at index {col_wrong_index}")
    wrong_list_index = list(set(wrong_list_index))
    logger.info("Origin data has wrong list index "
                "{wrong_index}".format(wrong_index=", ".join(list(map(str, wrong_list_index)))))

    wrong_conflict_index = list()
    check_cols_conflict = [["Company ID", "Company Name"],
                           ["Company Name", "Company ID"]]
    for conflict in check_cols_conflict:
        wrong_conflict_value = list()
        check_res = check_col_conflict(df=data_origin_type_correct,
                                       col1=conflict[0],
                                       col2=conflict[1])
        wrong_conflict_value += list(check_res[check_res > 1].index)
        col_wrong_index = list(data_origin_type_correct.loc[
                                   data_origin_type_correct[conflict[0]].isin(wrong_conflict_value)].index)
        if len(wrong_conflict_value) > 0:
            logger.info(f"{conflict[0]}: {wrong_conflict_value} has multi value of {conflict[1]}")
        wrong_conflict_index += col_wrong_index
    logger.info("Origin data has wrong conflict index "
                "{wrong_index}".format(wrong_index=", ".join(list(map(str, wrong_conflict_index)))))

    wrong_index = list(set(wrong_type_index + wrong_list_index
                           + wrong_range_index + wrong_conflict_index))
    data_origin_cleaned = drop_wrong_index_df(df=data_origin_filled,
                                              wrong_index_list=wrong_index)
    multi_index_list = []
    for k, v in {"Company Info": col_dict["Company Info"],
                 "Company Metric": col_dict["Company Metric"]}.items():
        multi_index_list += list(product([k], v))
    origin_multi_index = pd.MultiIndex.from_tuples(multi_index_list)
    data_origin_cleaned_2_excel = pd.DataFrame(columns=origin_multi_index)
    data_db_cleaned = origin_2_db(data_origin_cleaned)
    wrong_data_1 = get_wrong_data_db_from_origin(origin_data_df=data_origin.drop(index=wrong_index),
                                                 db_data=data_db_0)
    wrong_data_2 = get_wrong_data_origin_from_db(origin_data_df=data_origin.drop(index=wrong_index),
                                                 db_data=data_db_0)
    wrong_data = pd.concat([wrong_data_1, wrong_data_2])

    with pd.ExcelWriter("output/data_cleaned.xlsx",
                        mode="w") as writer:
        data_origin_cleaned_2_excel.to_excel(writer, sheet_name="data_origin_cleaned", startcol=0)
        data_origin_cleaned.to_excel(writer, sheet_name="data_origin_cleaned", index=False, startrow=1, startcol=1)
        data_db_cleaned.to_excel(writer, sheet_name="data_db_cleaned", index=False)
        wrong_data.to_excel(writer, sheet_name="answer", index=False)
