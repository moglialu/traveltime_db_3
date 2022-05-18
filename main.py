import time
start_time = time.time()
import sqlite3
conn = sqlite3.connect('gtfs_de.db')
c = conn.cursor()

def get_stop_times_temp(input_stop_name):
    input_stop_id = c.execute("SELECT stop_id FROM stops_t WHERE stop_name = ? ORDER BY stop_id ASC", [input_stop_name,]).fetchone()[0]
    input_stop_id = str(input_stop_id) + "%"
    c.execute("""CREATE TEMP TABLE routes_temp 
        AS SELECT DISTINCT route_id 
        FROM trips_t 
        WHERE trip_id IN
            (SELECT trip_id FROM stop_times_t WHERE stop_id LIKE ? )""", (input_stop_id,),).fetchall()
            #make table down a temporary one
    print (input_stop_id)
    c.execute("""CREATE  TEMP TABLE stop_times_temp 
        AS SELECT * 
        FROM stop_times_t 
        WHERE trip_id IN 
            (SELECT trip_id FROM trips_t WHERE route_id IN routes_temp)""") #alle trips der routen die durch x verlaufen
    return input_stop_id



input_stop_name = "Niederstetten, Bahnhof"
c.execute("DROP TABLE IF EXISTS temp4")

input = get_stop_times_temp(input_stop_name)
print(input)

c.execute("CREATE TEMP TABLE temp2 AS SELECT trip_id, departure_time, stop_sequence FROM stop_times_temp WHERE stop_id LIKE ?", (input,))
c.execute("ALTER TABLE temp2 RENAME COLUMN departure_time TO start_time")
c.execute("ALTER TABLE temp2 RENAME COLUMN stop_sequence TO start_stop_sequence")
c.execute("CREATE TEMP TABLE temp3 AS SELECT * FROM stop_times_temp INNER JOIN temp2 USING (trip_id)")
c.execute("DELETE FROM temp3 WHERE stop_sequence < start_stop_sequence")
c.execute("""CREATE TABLE temp4 AS SELECT *, ROUND((JULIANDAY(arrival_time) - JULIANDAY(start_time)) * 1440) AS timedelta FROM temp3 """)





conn.commit()
conn.close()
print("My program took", time.time() - start_time, "to run")


