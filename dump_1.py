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
c.execute("DROP TABLE IF EXISTS temp5")
c.execute("DROP TABLE IF EXISTS temp5")
c.execute("DROP TABLE IF EXISTS temp6")
c.execute("DROP TABLE IF EXISTS test")


input = get_stop_times_temp(input_stop_name)
print(input)

c.execute("CREATE TEMP TABLE temp2 AS SELECT trip_id, departure_time, stop_sequence FROM stop_times_temp WHERE stop_id LIKE ?", (input,))
c.execute("ALTER TABLE temp2 RENAME COLUMN departure_time TO start_time")
c.execute("ALTER TABLE temp2 RENAME COLUMN stop_sequence TO start_stop_sequence")
c.execute("CREATE TEMP TABLE temp3 AS SELECT * FROM stop_times_temp INNER JOIN temp2 USING (trip_id)")
c.execute("DELETE FROM temp3 WHERE stop_sequence < start_stop_sequence")
c.execute("""CREATE TABLE temp4 AS SELECT *, ROUND((JULIANDAY(arrival_time) - JULIANDAY(start_time)) * 1440) AS timedelta, TIME(arrival_time, '+30 minutes') AS latest_transfer_time FROM temp3""")
c.execute("CREATE TABLE temp5 AS SELECT * FROM temp4 INNER JOIN stops_t USING (stop_id)")

# c.execute("CREATE TABLE IF NOT EXISTS test (trip_id, arrival_time, departure_time, stop_id, stop_sequence, route_id, direction_id)")
# data_a = c.execute("SELECT * FROM temp5 LIMIT 50")
# for a in data_a.fetchall():
#     stop_id = c.execute("SELECT stop_id FROM stops_t WHERE stop_name = ? ORDER BY stop_id ASC", [a[11],]).fetchone()[0]  
#     c.execute("""INSERT INTO test SELECT * FROM stop_times_t 
#          WHERE stop_id = ? AND route_id NOT IN (SELECT DISTINCT route_id FROM temp5) AND departure_time > ? AND departure_time < ?""", (stop_id[4],a[3],a[10]))


    #c.execute("INSERT INTO temp5 (trip_id) VALUES('1234')")

    # c.execute("SELECT arrival_time, start_time, timedelta, latest_transfer_time, stop_name FROM temp5 ")  




conn.commit()
conn.close()
print("My program took", time.time() - start_time, "to run")







