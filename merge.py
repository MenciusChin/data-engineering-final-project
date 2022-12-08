"""Module for COVID map and beds in use"""

""""""
"""COVID map for all weeks"""
""""""

import plotly.graph_objects as go 
import pandas as pd 
import psycopg

from credentials import DB_PASSWORD, DB_USER
from helper import change_na


# Connect to DB
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname=DB_USER,
    user=DB_USER, password=DB_PASSWORD
)
cur = conn.cursor()


# Make a new SAVEPOINT
with conn.transaction():
    # Update the NaN in the dataset with NULL
    change_na(cur, conn, "facility_reports", 
              "adult_icu_patients_confirmed_covid")
    change_na(cur, conn, "facility_reports", "inpatient_beds_occupied_covid")
    
    # Get the data for COVID map in each state
    cur.execute("SELECT report_date, state, SUM(total_covid_0) "
                "AS total_covid FROM "
                "(SELECT hospital_pk, adult_icu_patients_confirmed_covid "
                "+ inpatient_beds_occupied_covid AS total_covid_0 "
                "FROM facility_reports) AS a "
                "INNER JOIN "
                "(SELECT facility_id, state "
                "FROM facility_information) AS b "
                "ON a.hospital_pk = b.facility_id "
                "GROUP BY state; ")
    
    #cur.execute("SELECT state, SUM(total_covid_0) AS total_covid FROM (SELECT hospital_pk, adult_icu_patients_confirmed_covid + inpatient_beds_occupied_covid AS total_covid_0 FROM facility_reports) AS a INNER JOIN(SELECT facility_id, state FROM facility_information) AS b ON a.hospital_pk = b.facility_id GROUP BY state;")
    data_map = pd.DataFrame(cur.fetchall(), columns=["state", "beds"])

# Now we commit and close the entire transaction
conn.commit()
conn.close()

# Make the map
fig = go.Figure(data=go.Choropleth( 
    locations=data_map['state'], 
    z = data_map['beds'].astype(float), # Set fill color
    locationmode = 'USA-states', # Set the country 
    colorscale = 'Reds', # Set the legend color
    colorbar_title = "Number", # Legend title
)) 
 
fig.update_layout( 
    title_text = 'The number of COVID cases by state', # Map title 
    geo_scope='usa', # Set country
)

fig.write_html("/project/output") 


""""""
"""COVID map per week"""
""""""

import numpy as np

# Make a new SAVEPOINT
with conn.transaction():
    # Update the NaN in the dataset with NULL
    change_na(cur, conn, "facility_reports", \
              "adult_icu_patients_confirmed_covid")
    change_na(cur, conn, "facility_reports", "inpatient_beds_occupied_covid")
    
    # Get the data for COVID map in each state
    cur.execute("SELECT state, report_date, SUM(total_covid_0) "
                "AS total_covid FROM "
                "(SELECT hospital_pk, report_date, "
                "adult_icu_patients_confirmed_covid "
                "+ inpatient_beds_occupied_covid AS total_covid_0 "
                "FROM facility_reports "
                "GROUP BY report_date, hospital_pk) AS a "
                "INNER JOIN "
                "(SELECT facility_id, state "
                "FROM facility_information) AS b "
                "ON a.hospital_pk = b.facility_id "
                "GROUP BY state, report_date "
                "ORDER BY state;")
    
    data_map = pd.DataFrame(cur.fetchall(), columns=["state", "week", "beds"])

# Now we commit and close the entire transaction
conn.commit()
conn.close()

########## need to change the date to input
data_map_tw = data_map.iloc[np.where(
    data_map["week"]==pd.Timestamp("2022-10-21"))[0],:]

# Make the map
fig = go.Figure(data=go.Choropleth( 
    locations=data_map['state'], 
    z = data_map['beds'].astype(float), # Set fill color
    locationmode = 'USA-states', # Set the country 
    colorscale = 'Reds', # Set the legend color
    colorbar_title = "Number", # Legend title
)) 
 
fig.update_layout( 
    ########## need to change the date to input
    title_text = 'The number of COVID cases by state' + ': 2022-10-21', 
    geo_scope='usa', # Set country
)

fig.write_html("/project/output") 

""""""
"""Module for number of beds in use (question 4)"""
""""""

import matplotlib.pyplot as plt

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
# save the plot, but we might not need it in output
plt.savefig("data_used", dpi=300)


# Make the pie chart
fig,axes1 = plt.subplots(1,3)

# Funtion helping make pie charts
def pie_helper(fig, data, n, i):
    labels1 = ["All cases", "Covid cases"]
    sizes_1 = [data["all_used_beds"][n],data["covid_used_beds"][n]]
    sizes_1 = pd.Series(sizes_1)
    colors = ["lightskyblue", "steelblue"]
    explode = [0.04, 0.04]
    fig[i].pie(sizes_1, colors=colors,labels=labels1,explode=explode,
                autopct="%.2f%%",shadow=False,startangle=130,pctdistance=0.5)
    fig[i].axis("equal")
    fig[i].set_title(data["report_date"][n])
    
pie_helper(axes1, data_used, 0, 0)
pie_helper(axes1, data_used, 1, 1)
pie_helper(axes1, data_used, 2, 2)
fig.savefig("pie_chart_1", dpi=300)
fig,axes2 = plt.subplots(1,2)
pie_helper(axes2, data_used, 3, 0)
pie_helper(axes2, data_used, 4, 1)
# save the plot, but we might not need it in output
fig.savefig("pie_chart_2", dpi=200)


