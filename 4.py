"""Module for number of beds in use"""

import pandas as pd
import psycopg
import matplotlib.pyplot as plt
import numpy as np

from credentials import DB_PASSWORD, DB_USER
from helper import pie_helper

# Connect to DB
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname=DB_USER,
    user=DB_USER, password=DB_PASSWORD
)
cur = conn.cursor()


with conn.transaction():
    # Extract the number of beds in use    
    cur.execute("SELECT report_date, SUM(total_occupied_0) "
                "AS total_occupied_all, "
                "SUM(inpatient_beds_occupied_covid) "
                "AS total_occupied_covid FROM "
                "(SELECT report_date, inpatient_beds_occupied_covid, "
                "total_adult_hospital_beds_occupied + "
                "total_pediatric_hospital_beds_occupied + "
                "total_icu_beds_occupied + "
                "inpatient_beds_occupied_covid AS total_occupied_0 "
                "FROM facility_reports) AS a "
                "GROUP BY report_date "
                "ORDER BY report_date;")
    data_used = pd.DataFrame(
        cur.fetchall(), columns=["report_date", 
                                 "all_used_beds",
                                 "covid_used_beds"]
        )
    
conn.commit()
conn.close()


# Make the bar plot
plt.figure(figsize=(7,4))
labels = data_used["report_date"]
x = np.arange(len(labels))
width = 0.25
plt.bar(x-width/2, data_used["all_used_beds"], width, 
        label = "All cases", tick_label=labels, fc="aquamarine")
plt.bar(x+width/2, data_used["covid_used_beds"], width, 
        label = "Covid cases", fc="lightseagreen")
#plt.ylim(0, 550000)
plt.xlabel("Report date")
plt.ylabel("The number of occupied beds")
plt.legend(loc="best")
plt.title("The number of beds in use of all cases and covid cases")
plt.savefig("data_used", dpi=300)


# Make the pie chart
fig,axes1 = plt.subplots(1,3)
pie_helper(axes1, data_used, 0, 0)
pie_helper(axes1, data_used, 1, 1)
pie_helper(axes1, data_used, 2, 2)
fig.savefig("pie_chart_1", dpi=300)
fig,axes2 = plt.subplots(1,2)
pie_helper(axes2, data_used, 3, 0)
pie_helper(axes2, data_used, 4, 1)
fig.savefig("pie_chart_2", dpi=200)


