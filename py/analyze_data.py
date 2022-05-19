import pandas as pd
from typing import List, Dict, Union
from loguru import logger


def get_column_from_df(df: pd.DataFrame) -> List[str]:
    return list(df.columns)


def check_val_type(value,
                   value_type: type) -> bool:
    try:
        value_type(value)
        return True
    except Exception as e:
        logger.error(e)
        return False


def check_col_type(col_values: pd.Series,
                   value_type: type) -> pd.Series:
    return col_values.apply(check_val_type, args=(value_type,))


def check_num_range(num,
                    min_num,
                    max_num):
    if min_num <= num <= max_num:
        return True
    else:
        return False


def check_col_range(col_values: pd.Series,
                    min_num,
                    max_num):
    return col_values.apply(check_num_range, args=(min_num, max_num))


def check_list(value,
               value_list: list):
    if value in value_list:
        return True
    else:
        return False


def check_col_list(col_values: pd.Series,
                   value_list: list):
    return col_values.apply(check_list, args=(value_list, ))


def check_col_conflict(df: pd.DataFrame,
                       col1: str,
                       col2: str):
    return df.groupby([col1, col2]).size().groupby(col1).size()