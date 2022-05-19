import pandas as pd
from typing import Dict


def get_ddf_from_excel(file_path: str,
                       sheet_name: list) -> Dict[str, pd.DataFrame]:
    return pd.read_excel(file_path,
                         sheet_name=sheet_name)



