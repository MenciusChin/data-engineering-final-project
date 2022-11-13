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
          "staffed_icu_adult_patients_confirmed_covid_7_day_avg"]

# Hashed so serach faster
existing_ids = set(facility_ids[0]) if len(facility_ids) > 0 else {}

with conn.transaction():
    # Create counting variables
    num_info_inserted = 0
    num_info_updated = 0
    num_report_inserted = 0

    for index, row in data.iterrows():
        # First extract our target variables
        (hospital_pk, report_date, state, hospital_name,
         address, city, zipcode, fipscode, geocoded_hospital_address,
         total_adult_hospital_beds, total_pediatric_hospital_beds,
         total_adult_hospital_beds_occupied,
         total_pediatric_hospital_beds_occupied,
         total_icu_beds, total_icu_beds_occupied,
         inpatient_beds_occupied_covid,
         adult_icu_patients_confirmed_covid) = row[target]

        # Data cleaning process
        if (total_adult_hospital_beds < 0):
            total_adult_hospital_beds = None
        if (total_pediatric_hospital_beds < 0):
            total_pediatric_hospital_beds = None
        if (total_adult_hospital_beds_occupied < 0):
            total_adult_hospital_beds_occupied = None
        if (total_pediatric_hospital_beds_occupied < 0):
            total_pediatric_hospital_beds_occupied = None
        if (total_icu_beds < 0):
            total_icu_beds = None
        if (total_icu_beds_occupied < 0):
            total_icu_beds_occupied = None
        if (inpatient_beds_occupied_covid < 0):
            inpatient_beds_occupied_covid = None
        if (adult_icu_patients_confirmed_covid < 0):
            adult_icu_patients_confirmed_covid = None

        # If the hospital is new
        if (hospital_pk not in existing_ids):
            # INSERT INTO facility_information
            try:
                # Make a new SAVEPOINT
                with conn.transaction():
                    # Only insert when not in table
                    cur.execute("INSERT INTO facility_information ("
                                "facility_id, facility_name, "
                                "geocoded_hospital_address, "
                                "address, city, state, zipcode, "
                                "fipscode"
                                ") VALUES ("
                                "%(facility_id)s, %(facility_name)s, "
                                "%(geocoded_hospital_address)s, "
                                "%(address)s, %(city)s, %(state)s, "
                                "%(zipcode)s, %(fipscode)s"
                                ");",
                                {
                                    "facility_id": hospital_pk,
                                    "facility_name": hospital_name,
                                    "geocoded_hospital_address":
                                    geocoded_hospital_address,
                                    "address": address,
                                    "city": city,
                                    "state": state,
                                    "zipcode": zipcode,
                                    "fipscode": fipscode
                                })
            # If exception caught (any), rollback
            except Exception as e:
                print("Insertion into facility_information failed at row " +
                      str(index) + ":", e)
                data.iloc[index].to_csv("error_row.csv")
            else:
                num_info_inserted += 1

        # If the hospital is not new
        else:
            # Update facility_information
            try:
                # Make a new SAVEPOINT
                with conn.transaction():
                    cur.execute("UPDATE facility_information "
                                "SET geocoded_hospital_address = "
                                "%(geocoded_hospital_address)s, "
                                "fipscode = %(fipscode)s "
                                "WHERE facility_id = %(facility_id)s;",
                                {
                                    "geocoded_hospital_address":
                                    geocoded_hospital_address,
                                    "fipscode": fipscode,
                                    "facility_id": hospital_pk
                                })

            # If exception caught (any), rollback
            except Exception as e:
                print("Updating facility_information failed at row " +
                      str(index) + ":", e)
                data.iloc[index].to_csv("error_row.csv")

            else:
                num_info_updated += 1

        # Then INSERT INTO facility_reports
        try:
            # Make a new SAVEPOINT
            with conn.transaction():
                cur.execute("INSERT INTO facility_reports ("
                            "report_date, hospital_pk, "
                            "total_adult_hospital_beds, "
                            "total_pediatric_hospital_beds, "
                            "total_adult_hospital_beds_occupied, "
                            "total_pediatric_hospital_beds_occupied, "
                            "total_icu_beds, total_icu_beds_occupied, "
                            "inpatient_beds_occupied_covid, "
                            "adult_icu_patients_confirmed_covid"
                            ") VALUES ("
                            "TO_DATE(%(report_date)s, 'YYYY-MM-DD'), "
                            "%(hospital_pk)s, %(total_adult_hospital_beds)s, "
                            "%(total_pediatric_hospital_beds)s, "
                            "%(total_adult_hospital_beds_occupied)s, "
                            "%(total_pediatric_hospital_beds_occupied)s, "
                            "%(total_icu_beds)s, %(total_icu_beds_occupied)s, "
                            "%(inpatient_beds_occupied_covid)s, "
                            "%(adult_icu_patients_confirmed_covid)s"
                            ");",
                            {"report_date": report_date,
                             "hospital_pk": hospital_pk,
                             "total_adult_hospital_beds":
                             total_adult_hospital_beds,
                             "total_pediatric_hospital_beds":
                             total_pediatric_hospital_beds,
                             "total_adult_hospital_beds_occupied":
                             total_adult_hospital_beds_occupied,
                             "total_pediatric_hospital_beds_occupied":
                             total_pediatric_hospital_beds_occupied,
                             "total_icu_beds": total_icu_beds,
                             "total_icu_beds_occupied":
                             total_icu_beds_occupied,
                             "inpatient_beds_occupied_covid":
                             inpatient_beds_occupied_covid,
                             "adult_icu_patients_confirmed_covid":
                             adult_icu_patients_confirmed_covid})

        except Exception as e:
            print("Insertion into quality_reports failed at row " +
                  str(index) + ":", e)
            data.iloc[index].to_csv("error_row.csv")
        else:
            # No exception happened, so we continue
            num_report_inserted += 1

# now we commit the entire transaction
conn.commit()
print("Number of rows inserted into facility_information:", num_info_inserted)
print("Number of rows updated in facility_information:", num_info_updated)
print("Number of rows inserted into quality_reports:", num_report_inserted)
