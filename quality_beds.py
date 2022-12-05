"""Module for number of beds in use by hospital quality rating"""

import pandas as pd
import psycopg
import matplotlib.pyplot as plt

from credentials import DB_PASSWORD, DB_USER
from helper import change_na

# Connect to DB
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname=DB_USER,
    user=DB_USER, password=DB_PASSWORD
)
cur = conn.cursor()

with conn.transaction():
    # Update the NaN in the dataset with NULL
    change_na(cur, conn, "facility_reports", \
              "total_adult_hospital_beds_occupied")
    change_na(cur, conn, "facility_reports", \
              "total_pediatric_hospital_beds_occupied")
    change_na(cur, conn, "facility_reports", "total_icu_beds_occupied")
    change_na(cur, conn, "facility_reports", "inpatient_beds_occupied_covid")

    # Extract the number of beds in use by hospital quality rating    
    cur.execute("SELECT rating, SUM(total_occupied_0) AS total_occupied FROM "
                "(SELECT hospital_pk, total_adult_hospital_beds_occupied "
                "+ total_pediatric_hospital_beds_occupied + "
                "total_icu_beds_occupied + inpatient_beds_occupied_covid "
                "AS total_occupied_0 "
                "FROM facility_reports) AS a "
                "INNER JOIN "
                "(SELECT facility_id, rating "
                "FROM quality_ratings) AS b "
                "ON a.hospital_pk = b.facility_id "
                "GROUP BY rating "
                "ORDER BY rating; ")
    data_quality_beds = pd.DataFrame(cur.fetchall(), \
                                     columns=["Quality Rating", \
                                              "Number of occupied beds"])
conn.commit()
conn.close()

data_quality_beds.to_csv("data_quality_beds.csv")

# Since there are NA value in the rating column, we manually set x-axis
x = ["1", "2", "3", "4", "5", "No rating"]

# Make the bar plot
plt.figure(figsize=(7,4))
plt.bar(range(len(x)), \
              data_quality_beds["Number of occupied beds"], \
                  tick_label=x, fc="lightblue")
plt.xlabel("Quality rating level")
plt.ylabel("The number of occupied beds")
plt.title("The number of beds in use by hospital quality rating")
for a,b in zip(range(len(x)), data_quality_beds["Number of occupied beds"]):
    plt.text(a, b, '%.1f' %b, ha='center', va='bottom', fontsize=8)
plt.savefig("data_quality_beds", dpi=300)
