"""Module for loading HHS data into DB"""

import sys
import pandas as pd
from credentials import DB_USER, DB_PASSWORD

# Store credentials
username = DB_USER
password = DB_PASSWORD

# Load data from terminal input
data = pd.read_csv('data/hhs/' + sys.agrv[1])
