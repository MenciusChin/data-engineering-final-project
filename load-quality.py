"""Module for loading reports data into DB"""

import sys
import psycopg
import pandas as pd
from credentials import DB_USER, DB_PASSWORD


# Connect to DB
conn = psycopg.connect(
    host='sculptor.stat.cmu.edu', dbname=DB_USER,
    user=DB_USER, password=DB_PASSWORD
)
cur = conn.cursor()

# Load data from terminal input
data = pd.read_csv('data/quality/' + sys.argv[2])

# Extract columns we need 

"""
INSERT INTO facility_information (
    facility_id, facility_name, facility_type, emergency_service,
    address, city, state_name, zip-code, county
) VALUES (
)
"""

num_rows_inserted = 0
num_rows_updated = 0

# Make a new transaction for inserting quality reports
"""
INSERT INTO quality_ratings (
    rating_date, rating, facility_id
) VALUES (
    %(rating_data)s, %(rating)s, %(facility_id)s
)
"""
with conn.transaction():
    for row in data:
        try:
            # make a new SAVEPOINT
            with conn.transaction():
                # perhaps a bunch of reformatting and data manipulation goes here

                # now insert the data
                cur.execute("INSERT INTO countries (country_code, country_name) "
                            "VALUES (%(cc)s, %(cid)s)",
                            {"cc": "fr", "cid": "France"})
        except psycopg.errors.UniqueViolation as uv:
            # if an exception/error happens in this block, Postgres goes back to
            # the last savepoint upon exiting the `with` block
            print("insert failed thus update:", uv)
            # add additional logging, error handling here
            cur.execute("UPDATE countries "
                        "SET country_name = %(cid)s "
                        "WHERE country_code = %(cc)s",
                        {"cc": "fr", "cid": "France"})
            num_rows_updated += 1
        else:
            # no exception happened, so we continue without reverting the savepoint
            num_rows_inserted += 1

# now we commit the entire transaction
print(num_rows_inserted)
print(num_rows_updated)
conn.commit()
