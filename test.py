import pandas as pd
import numpy as np
import sqlite3
db_name = 'World_Economies.db'
sql_connection = sqlite3.connect(db_name)
table_attribs = ["Country", "GDP_USD_millions"]

table_name = 'Countries_by_GDP'
#query_statement = f"SELECT * FROM {table_name} WHERE Country in ('Ukraine', 'Greece', 'Russia')"
query_statement = f"SELECT * FROM {table_name} WHERE GDP_USD_billions <=200 and  GDP_USD_billions >=100 ORDER BY GDP_USD_billions desc"
query_output = pd.read_sql(query_statement, sql_connection)
print(query_statement)
print(query_output)
sql_connection.close()