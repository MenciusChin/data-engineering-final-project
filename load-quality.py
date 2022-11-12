"""Module for loading reports data into DB"""

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
data = pd.read_csv('data/quality/' + sys.argv[2])

# Get existing hospitals/facilities id
facility_ids = pd.read_sql_query("SELECT facility_id "
                                 "FROM facility_information", conn)

# Target variables
target = ['Facility ID', 'Facility Name', 'Hospital Type',
          'Emergency Services', 'Address', 'City', 'State',
          'ZIP Code', 'County Name', 'Hospital overall rating']
existing_ids = set(facility_ids['facility_id'])     # Hashed so serach faster

# Start transaction
with conn.transaction():
    # Create counting variables
    num_info_inserted = 0
    num_info_updated = 0
    num_quality_inserted = 0

    for index, row in data.iterrows():
        # First extract our target variables
        (facility_id, facility_name, facility_type, emergency_service,
         address, city, state_abbrev, zipcode, county, rating) = row[target]

        # First INSERT INTO facility_information
        try:
            # Make a new SAVEPOINT
            with conn.transaction():
                if (facility_id not in facility_ids):
                    # Only insert when not in table
                    cur.execute("INSERT INTO facility_information ("
                                "facility_id, facility_name, facility_type, "
                                "emergency_service, address, city, "
                                "state_abbrev, zipcode, county"
                                ") VALUES ("
                                "%(facility_id)s, %(facility_name)s, "
                                "%(facility_type)s, %(emergency_service)s, "
                                "%(address)s, %(city)s, %(state_abbrev)s, "
                                "%(zipcode)s, %(county)s"
                                ")",
                                {
                                    "facility_id": facility_id,
                                    "facility_name": facility_name,
                                    "facility_type": facility_type,
                                    "emergency_service": emergency_service,
                                    "address": address,
                                    "city": city,
                                    "state_abbrev": state_abbrev,
                                    "zipcode": zipcode,
                                    "county": county
                                })

                else:
                    # If already exists, update what it doesn't have
                    cur.execute("UPDATE facility_information "
                                "SET facility_type = %(facility_type)s, "
                                "emergency_service = %(emergency_service)s, "
                                "state_abbrev = %(state_abbrev)s, "
                                "county = %(county)s "
                                "WHERE facility_id = %(facility_id)s"
                                ")",
                                {
                                    "facility_type": facility_type,
                                    "emergency_service": emergency_service,
                                    "state_abbrev": state_abbrev,
                                    "county": county
                                })
                    num_info_updated += 1       # Update count

        # If exception caught (any), rollback
        except Exception as e:
            print("Insertion failed:", e)
        else:
            num_info_inserted += 0

        # Then INSERT INTO quality_ratings
        try:
            # Make a new SAVEPOINT
            with conn.transaction():
                # If rating is 'Not Avaliable'
                if (rating == 'Not Avaliable'):
                    # Insert NULL for rating
                    cur.execute("INSERT INTO quality_ratings ("
                                "rating_date, rating, facility_id"
                                ") VALUES ("
                                "TO_DATE(%(rating_date)s, 'YYYY-MM-DD'), "
                                "NULL, %(facility_id)s"
                                ")",
                                {"rating_date": sys.argv[1],
                                 "facility_id": facility_id})
                else:
                    # Insert the data based on the row value
                    cur.execute("INSERT INTO quality_ratings ("
                                "rating_date, rating, facility_id"
                                ") VALUES ("
                                "TO_DATE(%(rating_date)s, 'YYYY-MM-DD'), "
                                "%(rating)s, %(facility_id)s"
                                ")",
                                {"rating_date": sys.argv[1], "rating": rating,
                                 "facility_id": facility_id})
        except Exception as e:
            print("Insertion failed:", e)
        else:
            # No exception happened, so we continue
            num_quality_inserted += 1

# now we commit the entire transaction
conn.commit()
print('Number of rows inserted into facility_information:', num_info_inserted)
print('Number of rows updated in facility_information:', num_info_updated)
print('Number of rows inserted into quality_ratings:', num_quality_inserted)
