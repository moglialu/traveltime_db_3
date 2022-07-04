# traveltime_db_3

THIS IS WORK IN PROGRESS!
The motivation behind the project is not to publish it. Much more it is about learning to code.

The goal of this project is to create traveltime-maps for public transport based on gtfs data. 

A traveltime-map shows you which destinations you can reach from a given starting location. 
It should also give you the minimum time you need for the journey. 
Its not like the routing from google maps because its not a 1 to 1 search, but a 1 to n. 

There is already this realy nice page (https://direkt.bahn.guru) which does basicly what this code is supposed to do. 
I found this site while already coding.

Dependencies:
* Language used: Python / SQL
* Modules used: pandas sqlite3 folium time
* Data: gtfs
  In this repository there is a sample dataset for Frankfurt, Stuttgart and Niederstetten. For the whole of germany visit [opendata-oepnv.de](https://www.opendata-oepnv.de/ht/de/organisation/delfi/startseite?tx_vrrkit_view%5Bdataset_name%5D=deutschlandweite-sollfahrplandaten-gtfs&tx_vrrkit_view%5Baction%5D=details&tx_vrrkit_view%5Bcontroller%5D=View)

#First time use:
1. run create_db_template.py 
1. Run main.py
2. Enter 'Würzburg Hbf'

To implement gtfs for all Germany:
1. Download gtfs data from [opendata-oepnv.de](https://www.opendata-oepnv.de/ht/de/organisation/delfi/startseite?tx_vrrkit_view%5Bdataset_name%5D=deutschlandweite-sollfahrplandaten-gtfs&tx_vrrkit_view%5Baction%5D=details&tx_vrrkit_view%5Bcontroller%5D=View)
2. Place routes.txt, stop_times.txt, stops.txt, trips.txt into the gtfs/ Folder
3. Run create_db.py
4. Run main.py
5. Enter your starting location (no typos allowed yet, see stops.txt to find your station)

What it can't do right now. (But may in the future)---
* Right now the script shows you only german trains. So far a transit of trains is not implemented. 
  This is a planned feature but right now the calculation needs to much time.
* There is no GUI yet.
* Transport by bus is excluded for performance reasons.
* Right now you can't enter a specific time or date for your travel.
* A geographic radius is not implemented yet
* A lot of Bugfixes needs to be done
* For more Information: to-do.txt
