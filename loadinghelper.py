"""Helper module for loading-data"""

import pandas as pd


# Helper function for checking numeric NA values
def check_numeric_na(var):
    """Check if the input variable is negative value or NaN"""
    return None if (var is None or var < 0) else var


def check_geo(var):
    if pd.isna(var):
        return None, None
    else:
        return float(var.split(" ")[1][1:]), float(var.split(" ")[2][:-1])
