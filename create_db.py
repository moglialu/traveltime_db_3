import time
start_time = time.time()
import pandas as pd
import os
import sqlite3
conn = sqlite3.connect('gtfs_de.db')
c = conn.cursor()
conn.commit()


print("Importing GTFS-Files...")

files = os.listdir("gtfs/")
for x in files:
    df = pd.read_csv ('gtfs/' + x, index_col = 0)
    df.columns.str.strip()
    df.to_sql(x[0:-4], conn)   #save importet df as table without the .txt ending

print("GTFS-Files were imported successfully.")
print("Creating new tables...")

c.execute("CREATE TABLE IF NOT EXISTS routes_t AS SELECT * FROM routes WHERE route_type > 99 OR route_type < 3")
c.execute("CREATE TABLE IF NOT EXISTS trips_t AS SELECT * FROM trips WHERE route_id IN ( SELECT route_id FROM routes_t)")
c.execute("CREATE TABLE IF NOT EXISTS stop_times_t AS SELECT * FROM stop_times WHERE trip_id IN ( SELECT trip_id FROM trips_t)")
c.execute("CREATE TEMP TABLE IF NOT EXISTS stops_t_temp AS SELECT * FROM stops WHERE stop_id IN (SELECT stop_id FROM stop_times_t)")
c.execute("CREATE TEMP TABLE IF NOT EXISTS stops_loc1_temp AS SELECT * FROM stops WHERE location_type = 1")
c.execute("CREATE TABLE IF NOT EXISTS stops_t AS SELECT * FROM stops_t_temp UNION SELECT * FROM stops_loc1_temp")

print("New tables were created succsessfully. Job done!")

conn.close()
print("My program took", time.time() - start_time, "to run")
