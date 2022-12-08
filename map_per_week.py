"""Module for COVID map per week"""

import plotly.graph_objects as go 
import pandas as pd 
import numpy as np
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

fig.write_html("/Users/yuxuanma/Desktop/614project/test2/US.html") 


