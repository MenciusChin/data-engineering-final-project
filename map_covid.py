import plotly.graph_objects as go 
import pandas as pd 
import psycopg

from credentials import DB_PASSWORD, DB_USER



# Connect to DB
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname=DB_USER,
    user=DB_USER, password=DB_PASSWORD
)
cur = conn.cursor()


# Make a new SAVEPOINT
with conn.transaction():
    # Update the NaN in the dataset with NULL
    cur.execute("UPDATE facility_reports "
                "SET adult_icu_patients_confirmed_covid = NULL "
                "WHERE adult_icu_patients_confirmed_covid = 'NaN'; ")
    
    cur.execute("UPDATE facility_reports "
                "SET inpatient_beds_occupied_covid = NULL "
                "WHERE inpatient_beds_occupied_covid = 'NaN'; ")
    
    # Get the data for COVID map in each state
    cur.execute("SELECT state, SUM(total_covid_0) AS total_covid FROM "
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

#data_map.to_csv("map.csv")

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

fig.write_html("/Users/yuxuanma/Desktop/614project/test2/US.html") 
