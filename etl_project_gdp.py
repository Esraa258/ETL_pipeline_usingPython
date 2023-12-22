#import libraries
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
csv_path = r'C:\Users\esraa\Desktop\PythonProjects\ETL_project1/Countries_by_GDP.csv'
db_name = 'world_Economies.db'
table_name = 'countries_by_GDP'
#df = pd.DataFrame(columns=['country', 'GDP_USD_billion'])
table_attribs = ["Country", "GDP_USD_millions"]

########################## Extraction ##########################
def extract(url, table_attribs):
    #download the contents of the webpage in text format and store in a variable called data
    #1-Extract the web page as text
    html_page = requests.get(url).text
    
    # create a BeautifulSoup object using the BeautifulSoup constructor
    #2-Parse the text into an HTML object
    data = BeautifulSoup(html_page, 'html.parser')
    
    #3-Create an empty pandas DataFrame named df with columns as the table_attribs
    df = pd.DataFrame(columns=table_attribs)
    
    #4-Extract all 'tbody' attributes of the HTML object and then extract all the rows of the index 2 table using the 'tr' attribute
    #the position of the table is in index 2 . because the images are stored in tabular format. Hence, in the given webpage, our table is at the third position, or index 2
    #the variable tables gets the body of all the tables in the web page
    tables = data.find_all('tbody')
    #the variable rows gets all the rows of the first table
    rows = tables[2].find_all('tr')
    
    #5-Check the contents of each row, having attribute ‘td’, for the following conditions.
    #a.The row should not be empty
    #b.The first column should contain a hyperlink
    #c.The third column should not be '—'
    #Run a for loop and check the conditions using if statements
    
    #6-Store all entries matching the conditions in step 5 to a dictionary with keys the same as entries of table_attribs. Append all these dictionaries one by one to the dataframe.
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df



########################## Transformation ##########################

def transform(df):
    #1-Convert the contents of the 'GDP_USD_millions' column of df dataframe from currency format to floating numbers
    #a-Save the dataframe column as a list
    GDP_list = df["GDP_USD_millions"].tolist()
    #b-Iterate over the contents of the list and use split() and join() functions to convert the currency text into numerical text. Type cast the numerical text to float
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    
    #2-Divide all these values by 1000 and round it to 2 decimal places
    #Use the numpy.round() function for rounding
    #Assign the modified list back to the dataframe
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    
    #3-Modify the name of the column from 'GDP_USD_millions' to 'GDP_USD_billions' by using df.rename() function
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})


    return df




########################## Loading ##########################
#save the transformed dataframe to a CSV file
##pass the dataframe df and the CSV file path to the function load_to_csv() 
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)


#save the transformed dataframe as a table in a database
##This needs to be implemented in the function load_to_db(), which accepts the dataframe df, the connection object to the SQL database conn, and the table name variable table_name to be used
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)



########################## Querying the database table ##########################
#the pandas.read_sql() function used to run query on the database table
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


########################## Logging progress ##########################
#log_progress() funciton will be called multiple times throughout the execution of this code and will be asked to add a log entry in a .txt file, etl_project_log.txt
#datetime.now() function used to get the current timestamp
def log_progress(message):
    #timestamp_format --> determines the formatting of the time &date
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    #now --> capture the current time by calling datetime 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format)
    #pull that information together (by opening a file &writing the information to the file)
    # 'a' ---> all the data written will be appended to the existing information
    with open("./etl_project_log.txt","a") as f:
        #we are then able to attach a timestamp to each part of the process of when it begins &when it has completed 
        f.write(timestamp + ' : ' + message + '\n')



########################## Function calls ##########################
#        Task           -->       Log message on completion
#Declaring known values --> Preliminaries complete. Initiating ETL process.
# Log the initialization of the ETL process 
log_progress('Preliminaries complete. Initiating ETL process')


#Call extract() function --> Data extraction complete. Initiating Transformation process.
#call the extract_data function
#(The data received from this step will then be transferred to the 2nd step of transforming, after this has completed the data
# is then loaded into the target file)
#before & after every step the time and date for start and completion has been added
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')


#Call transform() function --> Data transformation complete. Initiating loading process.
df = transform(df)
log_progress('Data transformation complete. Initiating loading process')

#Call load_to_csv()  --> Data saved to CSV file.
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

#Initiate SQLite3 connection --> SQL Connection initiated.
sql_connection = sqlite3.connect('World_Economies.db')
log_progress('SQL Connection initiated.')

#Call load_to_db() --> Data loaded to Database as table. Running the query.
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')

#Call run_query() *  --> Process Complete.
#Query: Display only the entries with more than a 100 billion USD economy
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

#Close SQLite3 connection --> -
sql_connection.close()