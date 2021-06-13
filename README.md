# DataframeToDB

_____

This project is a personal non-profit project rather than accepting donations to buy more coffee and carry out more open source projects! 

If you want to donate:  https://ko-fi.com/juanretamales

## Introduction

note: for the moment, i create a function for tested the library, but i changed the way how to work because to use for controling a database and not only a table.

When to use:

- When you need save dataframes, for example when scraping a table
- You need shared a database estructure for use in proyects

___

`DataframeToDB` is an improved way to upload pandas dataframes to Microsoft SQL Server, MySQL, PostgreDB and support for other type of database.

`DataframeToDB` takes advantage of  SQLAlchemy. This allows for a much lighter weight import for writing  pandas dataframes to db server. 

## Installation

____

For instalation you can copy the dataframetodb folder into your proyect from github, or
```python
pip install dataframetodb
```

## Requirements

______

- Written for Python 3.8+
- numpy>=1.20.2
- pandas>=1.2.4
- python-dateutil>=2.8.1
- SQLAlchemy>=1.4.9
- **driver for db conection**

## Example

___

```python
from dataframetodb import table
import pandas as pd
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

## How to work

The Table of DataframeToDB is a custom class who generate a SQLAlchemy Table when you call getTable function.

## USAGE
For more information, you can view the documentation in github or view de documentation of code.
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

Thanx to:
Joseph Astrahan for the answer in stackoverflow 

## FAQ

### What databases does DataframeToDB support? 

The same as SQLAlchemy, for now they are SQLite, Postgresql, MySQL, Oracle, MS-SQL, Firebird, Sybase and others. But you should check [this link](https://www.sqlalchemy.org/features.html). 

### why use json and not pickle?

It really cost me to decide, while pickle is faster, json is more transparent and it is easier to make modifications and transfers of files between projects, also it is expected that json can be used not only in tables, if not, in the entire database.

### why did you decide to create this library? 

For scrapping projects it is very tedious to be configuring tables, I wanted something more automatic, I found one but it was only compatible with MS-SQL, and in projects that could not afford that database I decided to create this and create things that I felt were missing. 

### Cats or Dogs?

Pandas!!!