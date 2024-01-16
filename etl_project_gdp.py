'''
https://web.archive.org/web/20230902185326/
https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
'''

# Code for ETL operations on Country-GDP data

# Importing the required libraries

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def extract(url, table_attribs):
    ''' The purpose of this function is to extract the required
    information from the website and save it to a dataframe. The
    function returns the dataframe for further processing. '''
    #1: Extract the web page as text
    page = requests.get(url).text
    #2: Parse the text into an HTML object
    data = BeautifulSoup(page,'html.parser')
    #3: Create an empty pandas DataFrame named df with columns as the table_attribs
    df = pd.DataFrame(columns=table_attribs)
    #4: Extract all 'tbody' attributes of the HTML object and then extract all the rows of the index 2 table using the 'tr' attribute
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    #5: Check the contents of each row, having attribute ‘td’, for the following conditions
    for row in rows:
        # Extract all the td data objects in the row and save them to col
        col = row.find_all('td')
        if len(col)!=0:
            #a. The row should not be empty
            #b. The first column should contain a hyperlink.
            #c. The third column should not be '—'.
            if col[0].find('a') is not None and '—' not in col[2]:
                #Store all entries matching the conditions in step 5 to a dictionary with keys the same as entries of table_attribs.
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                #Append all these dictionaries one by one to the dataframe
                df = pd.concat([df,df1], ignore_index=True)
    return df
# Step 2: Transform information
def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    #1: Convert Currency format to float value
    #a. Save the dataframe column as a list
    GDP_list = df["GDP_USD_millions"].tolist()
    #b. Convert the currency text into numerical text.
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    #2: Divide values by 1000 and round it to 2 decimal places
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    #3: Modify the name of the column
    df["GDP_USD_millions"] = GDP_list
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df

# Step 3: Loading information
def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path.'''
    # Pass the dataframe df and the CSV file path
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe to as a database table
    with the provided name.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Step 4: Querying the database table
def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Step 5: Logging progress
def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file.'''
    ''' Here, you define the required entities and call the relevant 
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')    

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'

# Funtion calls
# Log the initialization of the ETL process 
log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)

# Log the completion of Extraction and initiation of Transfromation process
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)

# Log the completion of Transfromation and initiation of Loading process
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)

# Log the completion of the data
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('World_Economies.db')

# Log the beginning of the SQL Connection
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)

# Log the beginning of the Loading process 
log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

# Log the completion of the Loading process 
log_progress('Process Complete.')
# Close SQLite3 connection
sql_connection.close()

