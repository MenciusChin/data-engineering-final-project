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


# Make the bar plot
fig_4_bar = go.Figure()
fig_4_bar.add_trace(go.Bar(
    x=data_used['report_date'],
    y=data_used['all_used_beds'],
    text=data_used['all_used_beds'],
    textposition='auto',
    name='All cases',
    marker_color='#41D3BD'
))
fig_4_bar.add_trace(go.Bar(
    x=data_used['report_date'],
    y=data_used['covid_used_beds'],
    text=data_used['covid_used_beds'],
    textposition='auto',
    name='COVID cases',
    marker_color='#379392'
))

fig_4_bar.update_layout(
    title={
        "text":"Number of beds in use of all cases and covid cases",
        "y":0.88,
        "x":0.5,
        "xanchor":"center",
        "yanchor":"top"
    },
    plot_bgcolor='#D8E6E7')

fig_4_bar.show()

# Make the pie chart
labels = ['All cases', 'COVID cases']

# Define color sets of paintings
colors1 = ['#88dba3', '#cff0da']
colors2 = ['#4ea1d3', '#d8e9ef']
colors3 = ['#6a60a9', '#dedcee']
colors4 = ['#fbd14b', '#fffcf0']
colors5 = ['#ee6e9f', '#fec9c9']

# Create subplots, using 'domain' type for pie charts
specs = [[{'type':'domain'}, {'type':'domain'}, {'type':'domain'}], 
        [{'type':'domain'}, {'type':'domain'}, {'type':'domain'}]]
fig_4_pie = make_subplots(rows=2, cols=3, specs=specs)

# Define pie charts
fig_4_pie.add_trace(go.Pie(labels=labels, values=[data_used["all_used_beds"][0],
                    data_used["covid_used_beds"][0]], name=str(data_used['report_date'][0]),
                     marker_colors=colors1), 1, 1)
fig_4_pie.add_trace(go.Pie(labels=labels, values=[data_used["all_used_beds"][1],
                    data_used["covid_used_beds"][1]], name=str(data_used['report_date'][1]),
                     marker_colors=colors2), 1, 2)
fig_4_pie.add_trace(go.Pie(labels=labels, values=[data_used["all_used_beds"][2],
                    data_used["covid_used_beds"][2]], name=str(data_used['report_date'][2]),
                     marker_colors=colors3), 1, 3)
fig_4_pie.add_trace(go.Pie(labels=labels, values=[data_used["all_used_beds"][3],
                    data_used["covid_used_beds"][3]], name=str(data_used['report_date'][3]),
                     marker_colors=colors4), 2, 1)
fig_4_pie.add_trace(go.Pie(labels=labels, values=[data_used["all_used_beds"][4],
                    data_used["covid_used_beds"][4]], name=str(data_used['report_date'][4]),
                     marker_colors=colors5), 2, 2)

# Tune layout and hover info
fig_4_pie.update_traces(hole=.4, hoverinfo='label+percent+name', textinfo='none')
fig_4_pie.update_layout(
    title={
        "text":"Number of beds in use of all cases and covid cases",
        "y":0.88,
        "x":0.5,
        "xanchor":"center",
        "yanchor":"top"
    },
    showlegend=False,
    annotations=[dict(text='9-23', x=0.132, y=0.825, font_size=13, showarrow=False),
                 dict(text='9-30', x=0.866, y=0.825, font_size=13, showarrow=False),
                 dict(text='10-07', x=0.5, y=0.825, font_size=13, showarrow=False),
                 dict(text='10-14', x=0.5, y=0.18, font_size=13, showarrow=False),
                 dict(text='10-21', x=0.131, y=0.18, font_size=13, showarrow=False)]
    )

fig_4_pie = go.Figure(fig_4_pie)
fig_4_pie.show()