"""Module for loading HHS data into DB"""

import sys

import pandas as pd
import psycopg

from credentials import DB_PASSWORD, DB_USER


# Connect to DB
conn = psycopg.connect(
    host='sculptor.stat.cmu.edu', dbname=DB_USER,
    user=DB_USER, password=DB_PASSWORD
)
cur = conn.cursor()

# Load data from terminal input
data = pd.read_csv('data/hhs/' + sys.argv[1])

# Get existing hospitals/facilities id
# Seem that pd.read_sql doesn't work
cur.execute("SELECT facility_id FROM facility_information")
facility_ids = pd.DataFrame(cur.fetchall())
conn.commit()        # Commit here for the SELECT clause

# Target variables
target = ["hospital_pk", "collection_week", "state",
          "hospital_name", "address", "city", "zip",
          "fips_code", "geocoded_hospital_address",
          "all_adult_hospital_beds_7_day_avg",
          "all_pediatric_inpatient_beds_7_day_avg",
          "all_adult_hospital_inpatient_bed_occupied_7_day_avg",
          "all_pediatric_inpatient_bed_occupied_7_day_avg",
          "total_icu_beds_7_day_avg", "icu_beds_used_7_day_avg",
          "inpatient_beds_used_covid_7_day_avg",
          "staffed_icu_adults_patients_confirmed_covid_7_day_avg"]

# Hashed so serach faster
existing_ids = set(facility_ids[0]) if len(facility_ids) > 0 else {}

with conn.transaction():
    # Create counting variables
    num_info_inserted = 0
    num_info_updated = 0
    num_quality_inserted = 0

    for index, row in data.iterrows():
        # First extract our target variables
        (hospital_pk, report_date, state_abbrev, hospital_name,
         address, city, state_abbrev, zipcode, county, rating) = row[target]
