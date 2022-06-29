import time
start_time = time.time()
import pandas as pd
import os
import sqlite3


os.remove("gtfs_de.db") 
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


#routes_t
c.execute("CREATE TEMP TABLE IF NOT EXISTS routes_t AS SELECT * FROM routes WHERE route_type > 99 OR route_type < 3")
#trips_t
c.execute("CREATE TEMP TABLE IF NOT EXISTS trips_t AS SELECT * FROM trips WHERE route_id IN ( SELECT route_id FROM routes_t)")
#stop_times_t
c.execute("""CREATE TEMP TABLE IF NOT EXISTS stop_times_temp AS SELECT *, CASE WHEN  length(arrival_time) < 8 THEN '0' || arrival_time ELSE arrival_time END, 
    CASE WHEN  length(departure_time) < 8 THEN '0' || departure_time ELSE departure_time END FROM stop_times WHERE trip_id IN ( SELECT trip_id FROM trips_t)""")
c.execute("ALTER TABLE stop_times_temp DROP COLUMN arrival_time")
c.execute("ALTER TABLE stop_times_temp DROP COLUMN departure_time")
c.execute("ALTER TABLE stop_times_temp DROP COLUMN pickup_type")
c.execute("ALTER TABLE stop_times_temp DROP COLUMN drop_off_type")
c.execute("ALTER TABLE stop_times_temp DROP COLUMN stop_headsign")
c.execute("""ALTER TABLE stop_times_temp RENAME "CASE WHEN  length(arrival_time) < 8 THEN '0' || arrival_time ELSE arrival_time END" TO arrival_time""")
c.execute("""ALTER TABLE stop_times_temp RENAME "CASE WHEN  length(departure_time) < 8 THEN '0' || departure_time ELSE departure_time END" TO departure_time""")
c.execute("CREATE TEMP TABLE stop_times_t AS SELECT stop_times_temp.*, route_id, direction_id FROM stop_times_temp INNER JOIN trips_t USING (trip_id)")

#stops_t
c.execute("CREATE TEMP TABLE IF NOT EXISTS stops_t_temp AS SELECT * FROM stops WHERE stop_id IN (SELECT stop_id FROM stop_times_t)")
c.execute("CREATE TEMP TABLE IF NOT EXISTS stops_loc1_temp AS SELECT * FROM stops WHERE location_type = 1")
c.execute("CREATE TABLE IF NOT EXISTS stops_t AS SELECT * FROM stops_t_temp UNION SELECT * FROM stops_loc1_temp")
c.execute("ALTER TABLE stops_t DROP COLUMN stop_code")
c.execute("ALTER TABLE stops_t DROP COLUMN wheelchair_boarding")
c.execute("ALTER TABLE stops_t DROP COLUMN level_id")
c.execute("ALTER TABLE stops_t DROP COLUMN platform_code")
c.execute("ALTER TABLE stops_t DROP COLUMN location_type")
c.execute("ALTER TABLE stops_t DROP COLUMN parent_station")
c.execute("ALTER TABLE stops_t DROP COLUMN stop_desc")

#traveltime
c.execute("CREATE TEMP TABLE traveltime_temp AS SELECT * FROM stop_times_t INNER JOIN stops_t USING (stop_id)")
c.execute("CREATE TEMP TABLE duplicates AS SELECT * FROM traveltime_temp GROUP BY route_id, stop_name ORDER BY stop_name")
c.execute("CREATE TEMP TABLE route_count AS SELECT stop_name, COUNT(*) AS route_count FROM duplicates GROUP BY stop_name")
c.execute("CREATE TABLE traveltime AS SELECT * FROM traveltime_temp INNER JOIN route_count USING (stop_name)")
c.execute("CREATE INDEX index_traveltime ON traveltime (stop_name, trip_id, stop_sequence)")

#clear db
c.execute("DROP TABLE IF EXISTS agency")
c.execute("DROP TABLE IF EXISTS calendar")
c.execute("DROP TABLE IF EXISTS calendar_dates")
c.execute("DROP TABLE IF EXISTS frequencies")
c.execute("DROP TABLE IF EXISTS levels")
c.execute("DROP TABLE IF EXISTS pathways")
c.execute("DROP TABLE IF EXISTS routes")
c.execute("DROP TABLE IF EXISTS shapes")
c.execute("DROP TABLE IF EXISTS stop_times")
c.execute("DROP TABLE IF EXISTS stops")
c.execute("DROP TABLE IF EXISTS transfers")
c.execute("DROP TABLE IF EXISTS trips")


print("New tables were created succsessfully. Job done!")

conn.close()
print("My program took", time.time() - start_time, "to run")

