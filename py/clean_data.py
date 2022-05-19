import pandas as pd
from typing import List, Union, Dict


def reset_cols_df(df: pd.DataFrame,
                  new_columns: Union[List[str], int]) -> pd.DataFrame:
    if isinstance(new_columns, int):
        df.columns = df.iloc[new_columns, :]
        df = df.drop(index=new_columns)
    else:
        df.columns = new_columns
    return df


def replace_none(df: pd.DataFrame,
                 cols_none_dict: Dict[str, List]):
    for col, cols_none in cols_none_dict.items():
        df[col] = df[col].replace(cols_none[0], cols_none[1])
    return df


def drop_wrong_index_df(df: pd.DataFrame,
                        wrong_index_list: List):
    return df.drop(index=wrong_index_list)


def reset_cols_type_df(df: pd.DataFrame,
                       cols_type: Dict[str, Union[str, type]]) -> pd.DataFrame:
    try:
        return df.astype(cols_type)
    except Exception as e:
        print(e)
        return None


