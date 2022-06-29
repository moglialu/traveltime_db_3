import time
import webbrowser
import folium
import pandas as pd
import sqlite3
conn = sqlite3.connect('gtfs_de.db')
c = conn.cursor()

#functions---------------------------------
def write_csv(name):
    c.execute("SELECT stop_name, stop_lat, stop_lon, timedelta FROM results_fastest")
    col_name_list = [tuple[0] for tuple in c.description]
    c.execute("SELECT stop_name, stop_lat, stop_lon, timedelta FROM results_fastest")
    result = c.fetchall()
    df = pd.DataFrame(result, columns = col_name_list)
    df.columns.str.strip()
    df.to_csv('results_csv/'+ name ,',', conn)

def get_traveltime_1(stop_name): 
    c.execute("DROP TABLE IF EXISTS results")
    c.execute("DROP TABLE IF EXISTS results_fastest")
    c.execute("""CREATE TABLE results (trip_id, stop_id, stop_sequence, arrival_time, departure_time, route_id, direction_id, stop_name, stop_lat, stop_lon, route_count, start_time, start_sequence, timedelta)""")

    c.execute("""CREATE TEMP TABLE start_temp AS SELECT trip_id, departure_time as start_time, stop_sequence as start_sequence FROM traveltime WHERE stop_name = ?""", (stop_name,)) 
    c.execute("""INSERT INTO results 
        SELECT *, ROUND((JULIANDAY(arrival_time) - JULIANDAY(start_time)) * 1440) AS timedelta 
        FROM traveltime INNER JOIN start_temp USING (trip_id) WHERE trip_id IN (SELECT trip_id FROM start_temp) AND stop_sequence > start_sequence;
    """)
    c.execute("""CREATE TABLE results_fastest AS SELECT *, min(timedelta) FROM results GROUP BY stop_name """)
    c.execute("""UPDATE results_fastest SET timedelta = 0 WHERE timedelta IS NULL""")#  temporary: start_time after 24:00:00 results in NULL and gives an error when convert to float with pandas


#user input-------------------------------------
input_location = input ("Enter your starting Location: ")
start_time = time.time()
print('Starting...')

#reading database-------------------------------
get_traveltime_1(input_location)
write_csv('map_csv')
print("List created.", time.time() - start_time,)

#pandas-----------------------------------------
map = folium.Map( location = [50.015, 10.067], zoom_start = 7 )
stops = pd.read_csv('results_csv/map_csv')
stops['timedelta'].astype(str).astype(float)
stops.loc[stops['timedelta'] == 0, 'color'] = 'gray'
stops.loc[(stops['timedelta'] > 0) & (stops['timedelta'] <= 15), 'color'] = 'lightgreen'
stops.loc[(stops['timedelta'] > 15) & (stops['timedelta'] <= 30), 'color'] = 'green'
stops.loc[(stops['timedelta'] > 30) & (stops['timedelta'] <= 60), 'color'] = 'darkgreen'
stops.loc[(stops['timedelta'] > 60) & (stops['timedelta'] <= 120), 'color'] = 'beige'
stops.loc[(stops['timedelta'] > 120) & (stops['timedelta'] <= 240), 'color'] = 'lightred'
stops.loc[(stops['timedelta'] > 240) & (stops['timedelta'] <= 360), 'color'] = 'red'
stops.loc[stops['timedelta'] > 360, 'color'] = 'darkred'

#mapping--------------------------------------
for _, stop in stops.iterrows():
    folium.Marker(
        location = [stop['stop_lat'], stop ['stop_lon']],
        popup=stop['stop_name'] + ", Time: " + str(stop ['timedelta']),
        tooltip= stop['stop_name'] + ", Time: " + str(stop ['timedelta']),
        icon=folium.Icon(color=stop['color'])
    ).add_to(map)
map.save('map.html')

webbrowser.open_new('map.html') #this does only work when running the script outside of vscode
input_location = input ("Map was created. Open 'map.html' if map did not open. Enter any key to close")

conn.commit()
conn.close()
print("My program took", time.time() - start_time, "to run")


