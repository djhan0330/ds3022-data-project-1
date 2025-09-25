import duckdb
import logging

con = duckdb.connect("nyc_taxi.duckdb")
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("Set s3_region = 'us-east-1';")

