# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    
    log_file = "./code_log.txt"
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n') 




def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)

def load_to_csv(df, csv_path):

    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
     
    df.to_csv(csv_path, sep='\t', encoding='utf-8', index=False, header=True)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists = 'replace', index =False)

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs).astype(table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[1].find('a') is not None:
                data_dict = {"Name": col[1].contents[2],
                            "MC_USD_Billion": float(col[2].contents[0][:-1])}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    rate_df = pd.read_csv('exchange_rate.csv')
    exchange_rate = rate_df.set_index('Currency').to_dict()['Rate']
    
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = {"Name":str, "MC_USD_Billion":float}
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
log_progress("Preliminaries complete. Initiating ETL process.")
df = extract(url, table_attribs)

log_progress("Data extraction complete. Initiating Transformation process")
df = transform(df)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, csv_path)
log_progress("Data saved to CSV file.")

sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated.")
load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as table. Running the query.")
query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion)  from {table_name}"
run_query(query_statement, sql_connection)
query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)
log_progress("Process Complete.")
sql_connection.close()
log_progress("Server Connection closed")

