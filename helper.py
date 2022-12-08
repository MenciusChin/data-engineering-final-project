import pandas as pd

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