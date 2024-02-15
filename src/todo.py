import pandas as pd
import hashlib
import pyodbc
import concurrent.futures
from zipfile import ZipFile

# Function to calculate hash
def calculate_hash(row):
    return hashlib.sha256(row.values.tobytes()).hexdigest()

# Function to insert data into SQL Server
def insert_data(df, conn_str):
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO YourTable VALUES (?, ?, ?)", row['column1'], row['column2'], row['hash'])
        conn.commit()

# Open the zip file
with ZipFile('your_file.zip', 'r') as zipObj:
    # Extract all the contents of zip file in current directory
    zipObj.extractall()

# Load the data
df = pd.read_csv('your_file.txt')

# Add a new column with the hash of the other columns
df['hash'] = df.apply(calculate_hash, axis=1)

# Connection string to your SQL Server database
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server;DATABASE=your_database;UID=your_username;PWD=your_password'

# Use ThreadPoolExecutor for parallel execution
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Split dataframe into chunks
    chunks = np.array_split(df, 100)  # Adjust the number of chunks as needed
    futures = [executor.submit(insert_data, chunk, conn_str) for chunk in chunks]

    for future in concurrent.futures.as_completed(futures):
        print(future.result())
