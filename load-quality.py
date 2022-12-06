"""Module for loading reports data into DB"""

import sys
import pandas as pd
import psycopg

from credentials import DB_PASSWORD, DB_USER
from loadinghelper import get_existing_ids, check_rating


# Connect to DB
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname=DB_USER,
    user=DB_USER, password=DB_PASSWORD
)
cur = conn.cursor()

# Load data from terminal input
data = pd.read_csv('data/quality/' + sys.argv[2])

existing_ids = get_existing_ids(cur, conn)

# Target variables
target = ["Facility ID", "Facility Name", "Hospital Type",
          "Hospital Ownership", "Emergency Services", "Address",
          "City", "State", "ZIP Code", "County Name",
          "Hospital overall rating"]
errors = pd.DataFrame(columns=target)

# Change rating to None if Not Avaliable
data["Hospital overall rating"] = data["Hospital overall rating"].\
    apply(check_rating)

# Start transaction
with conn.transaction():
    # Create counting variables
    num_info_inserted = 0
    num_info_updated = 0
    num_quality_inserted = 0

    for index, row in data.iterrows():
        # First extract our target variables
        (facility_id, facility_name, facility_type, ownership,
         emergency_service, address, city, state, zipcode, county,
         rating) = row[target]

        # If the hospital is new
        if (facility_id not in existing_ids):
            # INSERT INTO facility_information
            try:
                # Make a new SAVEPOINT
                with conn.transaction():
                    # Only insert when not in table
                    cur.execute("INSERT INTO facility_information ("
                                "facility_id, facility_name, address, city, "
                                "state, zipcode, county"
                                ") VALUES ("
                                "%(facility_id)s, %(facility_name)s, "
                                "%(address)s, %(city)s, %(state)s, "
                                "%(zipcode)s, %(county)s"
                                ");",
                                {
                                    "facility_id": facility_id,
                                    "facility_name": facility_name,
                                    "address": address,
                                    "city": city,
                                    "state": state,
                                    "zipcode": zipcode,
                                    "county": county
                                })
            # If exception caught (any), rollback
            except Exception as e:
                print("Insertion into facility_information failed at row " +
                      str(index) + ":", e)
                errors = pd.concat(
                    [errors, pd.DataFrame(row[target]).transpose()],
                    ignore_index=True
                    )
            else:
                num_info_inserted += 1

        # If the hospital is not new
        else:
            # Update facility_information
            try:
                # Make a new SAVEPOINT
                with conn.transaction():
                    cur.execute("UPDATE facility_information "
                                "SET county = %(county)s "
                                "WHERE facility_id = %(facility_id)s;",
                                {
                                    "county": county,
                                    "facility_id": facility_id
                                })

            # If exception caught (any), rollback
            except Exception as e:
                print("Updating facility_information failed at row " +
                      str(index) + ":", e)
                errors = pd.concat(
                    [errors, pd.DataFrame(row[target]).transpose()],
                    ignore_index=True
                    )

            else:
                num_info_updated += 1

        # Then INSERT INTO quality_ratings
        try:
            # Make a new SAVEPOINT
            with conn.transaction():
                cur.execute("INSERT INTO quality_ratings ("
                            "rating_date, rating, facility_type, "
                            "ownership, emergency_service, facility_id"
                            ") VALUES ("
                            "TO_DATE(%(rating_date)s, 'YYYY-MM-DD'), "
                            "%(rating)s, %(facility_id)s"
                            ");",
                            {"rating_date": sys.argv[1],
                             "rating": rating,
                             "facility_type": facility_type,
                             "ownership": ownership,
                             "emergency_service": emergency_service,
                             "facility_id": facility_id
                             })
        except Exception as e:
            print("Insertion into quality_ratings failed at row " +
                  str(index) + ":", e)
            errors = pd.concat([errors, pd.DataFrame(row[target]).transpose()],
                               ignore_index=True)

        else:
            # No exception happened, so we continue
            num_quality_inserted += 1

# now we commit the entire transaction
conn.commit()
conn.close()

errors.to_csv("error_rows.csv")
print("Number of rows inserted into facility_information:", num_info_inserted)
print("Number of rows updated in facility_information:", num_info_updated)
print("Number of rows inserted into quality_ratings:", num_quality_inserted)
