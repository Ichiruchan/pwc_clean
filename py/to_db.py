import pandas as pd
from config.data_config import col_dict

db_col = col_dict["Company Info"] + col_dict["Db Col"]


def origin_2_db(df: pd.DataFrame):
    ret_df = pd.DataFrame(columns=db_col)
    for row_df in df.apply(origin_row_2_db, axis=1).values:
        ret_df = pd.concat([ret_df, row_df])
    return ret_df


def origin_row_2_db(row: pd.Series) -> pd.DataFrame:
    ret_df = pd.DataFrame(columns=db_col)
    for metric_name in ["SP", "CDS", "APD",	"ARD", "ADA"]:
        ret_df.loc[ret_df.shape[0], col_dict["Company Info"]] = row[col_dict["Company Info"]]
        ret_df.loc[ret_df.shape[0] - 1, col_dict["Db Col"]] = [metric_name, row[metric_name]]
    return ret_df
