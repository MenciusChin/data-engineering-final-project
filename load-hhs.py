"""Module for loading HHS data into DB"""

import sys
import pandas as pd
from credentials import DB_USER, DB_PASSWORD
import psycopg

# Store credentials
username = DB_USER
password = DB_PASSWORD

# Connect to DB
conn = psycopg.connect(
    host='sculptor.stat.cmu.edu', dbname=DB_USER,
    user=DB_USER, password=DB_PASSWORD
)
cur = conn.cursor()

# Load data from terminal input
data = pd.read_csv('data/hhs/' + sys.argv[1])

# Target variables
target = ["collection_week","hospital_pk", "hospital_name", 
          "all_adult_hospital_beds_7_day_avg",
          "all_pediatric_inpatient_beds_7_day_avg", 
          "all_adult_hospital_inpatient_bed_occupied_7_day_coverage", 
          "all_pediatric_inpatient_bed_occupied_7_day_avg", 
          "total_icu_beds_7_day_avg", "icu_beds_used_7_day_avg", 
          "inpatient_beds_used_covid_7_day_avg", 
          "staffed_adult_icu_patients_confirmed_covid_7_day_avg"]

# Start transaction
with conn.transaction():
    # Create counting variables
    num_repo_inserted = 0

    for index, row in data.iterrows():
        # First extract our target variables
        (report_date, hospital_pk, hospital_name, total_adult_hospital_beds,
         total_pediatric_hospital_beds, total_adult_hospital_beds_occupied,
         total_pediatric_hospital_beds_occupied, total_icu_beds,
         total_icu_beds_occupied, inpatient_beds_occupied_covid,
         adult_icu_patients_confirmed_covid) = row[target]

        # Convert -999 to None
        if (total_adult_hospital_beds == "-999999"):
            total_adult_hospital_beds = None
        if (total_pediatric_hospital_beds == "-999999"):
            total_pediatric_hospital_beds = None
        if (total_adult_hospital_beds_occupied == "-999999"):
            total_adult_hospital_beds_occupied = None
        if (total_pediatric_hospital_beds_occupied == "-999999"):
            total_pediatric_hospital_beds_occupied = None
        if (total_icu_beds == "-999999"):
            total_icu_beds = None
        if (total_icu_beds_occupied == "-999999"):
            total_icu_beds_occupied = None
        if (inpatient_beds_occupied_covid == "-999999"):
            inpatient_beds_occupied_covid = None 
        if (adult_icu_patients_confirmed_covid == "-999999"):
            adult_icu_patients_confirmed_covid = None 

        # Insert into facility_reports
        try:
            cur.execute("INSERT INTO facility_reports ("
                        "report_date", "hospital_pk", "hospital_name", 
                        "total_adult_hospital_beds",
                        "total_pediatric_hospital_beds", 
                        "total_adult_hospital_beds_occupied",
                        "total_pediatric_hospital_beds_occupied", 
                        "total_icu_beds","total_icu_beds_occupied", 
                        "inpatient_beds_occupied_covid",
                        "adult_icu_patients_confirmed_covid"
                        ") VALUES ("
                        "TO_DATE(%(report_date)s, 'YYYY-MM-DD'), "
                        "%(hospital_pk)s, %(hospital_name)s, "
                        "%(total_adult_hospital_beds)s, "
                        "%(total_pediatric_hospital_beds)s, "
                        "%(total_adult_hospital_beds_occupied)s, "
                        "%(total_pediatric_hospital_beds_occupied)s, "
                        "%(total_icu_beds)s, "
                        "%(total_icu_beds_occupied)s"
                        "%(inpatient_beds_occupied_covid)s"
                        "%(adult_icu_patients_confirmed_covid)s"
                        ");",
                        {
                            "report_date": report_date,
                            "hospital_pk": hospital_pk,
                            "hospital_name": hospital_name,
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
                                adult_icu_patients_confirmed_covid                                       
                        })
        # If exception caught (any), rollback
        except Exception as e:
            print("Insertion into facility_reports failed at row " +
                  str(index) + ":", e)
            data.iloc[index].to_csv("error_row_hhs.csv")
        else:
            num_repo_inserted += 1



# now we commit the entire transaction
conn.commit()
print("Number of rows inserted into facility_reports:", num_repo_inserted)


