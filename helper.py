

def change_na(cur, conn, table, var):
    """
    Update the NaN in the dataset with NULL
    
    Psycopg objects:
    cur -- cursor object
    conn -- connection object"""
    
    # Update the NaN in the dataset with NULL
    cur.execute("UPDATE {0} "
                "SET {1} = NULL "
                "WHERE {1} = 'NaN'; ".format(table, var))