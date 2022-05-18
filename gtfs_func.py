import time
import os
import sqlite3
import pandas as pd
start_time = time.time()
conn = sqlite3.connect('gtfs_de.db')
c = conn.cursor()

def get_timedelta(time_arrival, time_departure):
    h1, m1, s1 = time_arrival.split(":")
    h2, m2, s2 = time_departure.split(":")
    seconds_arrival = int(h1) * 3600 + int(m1) * 60 + int(s1)
    seconds_departure = int(h2) * 3600 + int(m2) * 60 + int(s2)
    time_delta = seconds_arrival - seconds_departure
    return time.strftime("%H:%M:%S", time.gmtime(time_delta))


def print_query(query):
    data= c.execute(query)
    for y in data.fetchall():
        print(y)


# returns stop_id of station
def get_stop_id(input_stop_name):
    input_stop_id = c.execute("SELECT stop_id FROM stops WHERE location_type = 1 AND stop_name IS ?", [input_stop_name,])
    return (input_stop_id.fetchone()[0])


#returns trip_ids of station
def get_trip_id(input_stop_id):
    input_stop_id = str(input_stop_id) + "%"
    input_trip_id = c.execute("SELECT trip_id FROM stop_times_t WHERE stop_id LIKE '?' ", (input_stop_id,),).fetchall()
    return input_trip_id


#returns route_ids of station
def get_route_id(input_stop_id):
    input_stop_id = str(input_stop_id) + "%"
    input_route_id = c.execute("SELECT DISTINCT route_id FROM trips_t WHERE trip_id IN (SELECT trip_id FROM stop_times_t WHERE stop_id LIKE ? )", (input_stop_id,),).fetchall()
    return input_route_id


def get_stop_times_temp(input_stop_name):
    input_stop_id = c.execute("SELECT stop_id FROM stops WHERE location_type = 1 AND stop_name IS ?", [input_stop_name,]).fetchone()[0]
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
            (SELECT trip_id FROM trips_t WHERE route_id IN routes_temp)""")
    return input_stop_id


#get stop_names of a route
def get_stop_name_from_route(route_id):
    stop_ids = c.execute("SELECT stop_name FROM stops WHERE stop_id IN (SELECT DISTINCT stop_id FROM stop_times_t WHERE trip_id IN (SELECT trip_id FROM trips_t WHERE route_id = ?))", (route_id,),).fetchall()
    return(stop_ids)


def show_tables():
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(c.fetchall())


# not tested yet
def create_gtfs_db(directory):
    files = os.listdir(directory)
    for x in files:
        df = pd.read_csv (directory + x, index_col = 0)
        df.columns.str.strip()
        df.to_sql(x[0:-4], conn)   #save importet df as table without the .txt ending


def search_completion(input):
    input = input + "%"
    proposal = c.execute("SELECT DISTINCT stop_name FROM stops_t WHERE stop_name LIKE ? ORDER BY stop_name ASC", (input,))
    for y in proposal.fetchall():
        print(y)

       


