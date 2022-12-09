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

# Additional I - COVID map
with conn.transaction():
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
    
    data_map2 = pd.DataFrame(cur.fetchall(), columns=["state", "week", "beds"])


data_map_tw = data_map2.iloc[np.where(
    data_map2["week"]==pd.Timestamp(week))[0],:]

# Make the map
fig_map2 = go.Figure(data=go.Choropleth( 
    locations=data_map_tw['state'], 
    z = data_map_tw['beds'].astype(float), # Set fill color
    locationmode = 'USA-states', # Set the country 
    colorscale = 'Reds', # Set the legend color
    colorbar_title = "Number", # Legend title
)) 

fig_map2.update_layout(
    title={
        "text":"The number of COVID cases by state" + ": " + week,
        "y":0.95,
        "x":0.5,
        "xanchor":"center",
        "yanchor":"top"
    },
    geo_scope='usa', # Set country
    autosize=True,
    margin=dict(t=30,b=25,l=0,r=0)
)
