import numpy as np
import pandas as pd
from config.data_config import col_dict
from loguru import logger


def get_wrong_data_db_from_origin(origin_data_df: pd.DataFrame,
                                  db_data: pd.DataFrame):
    ret_df = pd.DataFrame(columns=col_dict["Company Info"]+["Company Metric", "Data in DB", "Data in File", "ERROR Type"])
    for row_df in origin_data_df.apply(get_wrong_data_db, args=(db_data,), axis=1).values:
        ret_df = pd.concat([ret_df, row_df])
    return ret_df


def get_wrong_data_db(origin_data: pd.Series,
                      db_data: pd.DataFrame):
    ret_df = pd.DataFrame(columns=col_dict["Company Info"]+["Metric Name", "Data in DB", "Data in File", "ERROR Type"])
    origin_info = origin_data[col_dict["Company Info"]]
    db_index_list = set(db_data.index)
    for info in col_dict["Company Info"]:
        curr_db_index = set(db_data.loc[db_data[info] == origin_info[info]].index)
        db_index_list = db_index_list.intersection(curr_db_index)
        if len(db_index_list) == 0:
            for metric_name in ["SP", "CDS", "APD", "ARD", "ADA"]:
                ret_df.loc[ret_df.shape[0], col_dict["Company Info"]] = origin_info
                ret_df.loc[ret_df.shape[0]-1, "Metric Name"] = metric_name
                ret_df.loc[ret_df.shape[0]-1, "Data in File"] = origin_data[metric_name]
                ret_df.loc[ret_df.shape[0]-1, "ERROR Type"] = "Not_in_DB"
            return ret_df
    cand_db_data = db_data.loc[list(db_index_list), :]
    for metric_name in ["SP", "CDS", "APD", "ARD", "ADA"]:
        try:
            db_value = cand_db_data.loc[cand_db_data["Metric Name"] == metric_name, "Value"].iloc[0]
            origin_value = origin_data[metric_name]
            if not np.isnan(db_value):
                db_value = int(db_value)
            if not np.isnan(origin_value):
                origin_value = int(origin_value)
            if np.isnan(db_value) and np.isnan(origin_value):
                continue
            if not db_value == origin_value:
                ret_df.loc[ret_df.shape[0], col_dict["Company Info"]] = origin_info
                ret_df.loc[ret_df.shape[0] - 1, "Metric Name"] = metric_name
                ret_df.loc[ret_df.shape[0] - 1, "Data in File"] = origin_value
                ret_df.loc[ret_df.shape[0] - 1, "Data in DB"] = db_value
                ret_df.loc[ret_df.shape[0] - 1, "ERROR Type"] = "UnEqual"
        except Exception as e:
            logger.info(f"{origin_data['Company ID']} + {e}")
            ret_df.loc[ret_df.shape[0], col_dict["Company Info"]] = origin_info
            ret_df.loc[ret_df.shape[0] - 1, "Metric Name"] = metric_name
            ret_df.loc[ret_df.shape[0] - 1, "Data in File"] = origin_data[metric_name]
            ret_df.loc[ret_df.shape[0] - 1, "ERROR Type"] = "Not_in_DB"
    return ret_df


def get_wrong_data_origin_from_db(origin_data_df: pd.DataFrame,
                                  db_data: pd.DataFrame):
    ret_df = pd.DataFrame(columns=col_dict["Company Info"]+["Company Metric", "Data in DB", "Data in File", "ERROR Type"])
    for row_df in db_data.apply(get_wrong_data_origin, args=(origin_data_df,), axis=1).values:
        ret_df = pd.concat([ret_df, row_df])
    return ret_df


def get_wrong_data_origin(db_data: pd.Series,
                          origin_data: pd.DataFrame):
    ret_df = pd.DataFrame(columns=col_dict["Company Info"]+["Metric Name", "Data in DB", "Data in File", "ERROR Type"])
    db_info = db_data[col_dict["Company Info"]]
    origin_index_list = set(origin_data.index)
    for info in col_dict["Company Info"]:
        curr_origin_index = set(origin_data.loc[origin_data[info] == db_info[info]].index)
        origin_index_list = origin_index_list.intersection(curr_origin_index)
        if len(origin_index_list) == 0:
            ret_df.loc[ret_df.shape[0], col_dict["Company Info"]] = db_info
            ret_df.loc[ret_df.shape[0] - 1, "Metric Name"] = db_data["Metric Name"]
            ret_df.loc[ret_df.shape[0] - 1, "Data in DB"] = db_data["Value"]
            ret_df.loc[ret_df.shape[0] - 1, "ERROR Type"] = "Not_in_FILE"
            return ret_df