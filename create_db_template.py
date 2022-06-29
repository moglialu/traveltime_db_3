import pandas as pd
import sqlite3

conn = sqlite3.connect('gtfs_de.db')
c = conn.cursor()
conn.commit()

df = pd.read_csv ('traveltimetemplate.csv', index_col = 0)
df.to_sql('traveltime', conn, index=False)
c.execute("CREATE INDEX index_traveltime ON traveltime (stop_name, trip_id, stop_sequence)")

conn.close()

