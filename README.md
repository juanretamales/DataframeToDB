# DataframeToDB

_____

This project is a personal non-profit project rather than accepting donations to buy more coffee and carry out more open source projects! 

If you want to donate:  https://ko-fi.com/juanretamales

## Introduction

note: not use yet, in development in my freetime

___

`DataframeToDB` is an improved way to upload pandas dataframes to Microsoft SQL Server, MySQL, PostgreDB and support for other type of database.

`DataframeToDB` takes advantage of  SQLAlchemy. This allows for a much lighter weight import for writing  pandas dataframes to db server. 

## Installation

____

For instalation, for now, copy the file 'DataframeToDB.py into the project' and import

## Requirements

______

- Written for Python 3.8+
- Pandas library
- driver for db conection

## Example

___

```python
import pandas as pd
from DataframeToDB import dataframetodb as toDB
from datetime import datetime

import pyodbc
# Test Dataframe for insertion
df = pd.DataFrame({
    "Col1": [1, 2, 3],
    "Col2": ["A", "B", "C"],
    "Col3": [True, False, True],
    "Col4": [datetime(2020,1,1),datetime(2020,1,2),datetime(2020,1,3)]
})

# Create a pyodbc connection
conn = pyodbc.connect(
    """
    Driver={ODBC Driver 17 for SQL Server};
    Server=localhost;
    Database=my_database;
    UID=my_user;
    PWD=my_pass;
    """
)

# You can use this optional function for asign type data to dataframe and use the best way the library
df = toDB.refactor(df)

# If a table is created, the generated sql is returned
create_statement = toDB.save(df, "my_great_table", conn, if_exists="replace", custom={"Col1":"INT PRIMARY KEY"}, temp=False)

# Commit upload actions and close connection
conn.commit()
conn.close()

```

## USAGE

___

### SAVE

```python
toDB.save(df, name, conn, if_exists="append", custom=None, temp=False)
```

* ```df```: pandas DataFrame to upload
* ```name```: String of desired name for the table
* ```conn```: A valid connection object
* ```if_exists```: Option for what to do if the specified table name already exists in the database. If the table does not exist a new one will be created. By default this option is set to 'append'
  * __'append'__: Appends the dataframe to the table if it already exists.
  * __'fail'__: Purposely raises a `FailError` if the table already exists.
  * __'replace'__: Drops the old table with the specified name, and creates a new one. **Be careful with this option**, it will completely delete a table with the specified name.
  * 'upsert ': clean and insert
* ```custom```: A dictionary object with one or more of the column names being uploaded as the key, and a valid SQL column definition as the value. The value must contain a type (`INT`, `FLOAT`, `VARCHAR(500)`, etc.), and can optionally also include constraints (`NOT NULL`, `PRIMARY KEY`, etc.)
  * Examples: 
  `{'ColumnName':'varchar(1000)'}` 
  `{'ColumnName2':'int primary key'}`
* ```temp```: Either `True` if creating a local sql server temporary table for the connection, or `False` (default) if not.

inspired in fast-to-sql