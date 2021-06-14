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

## Example - Save dataframe into database

___

```python
import pandas as pd
import dataframetodb
from dataframetodb import Table, refactor
from datetime import datetime
import os

nametable = "nameTable"
engine = dataframetodb.create_engine('sqlite:///{}.sqlite'.format(nametable)) #create engine for use SQLAlchemy
df = pd.read_csv("./dataset/data.csv") # Get the DataFrame
df = refactor(df) # Returns a dataframe with the correct dtype compatible with DataframeToDB.
table_class = Table(name=nametable, df=df) #create Table instance
table_class.toDb(df, engine, 'append') #insert data in database, in this example sqlite
```

## How to work

The Table of DataframeToDB is a custom class who generate a SQLAlchemy Table when you call getTable function.

## USAGE
For more information, you can view [the documentation](https://github.com/juanretamales/DataframeToDB/blob/main/documentation.md) in [github](https://github.com/juanretamales/DataframeToDB) or view de documentation of code.
___

### Save data in database with toDB

```python
table_class.toDb(df.sample(50), engine, 'append')

"""
Insert data of dataframe into database (is necesary conection), and apply method for try create database Use insert function for add data to db

Parameters:
  df : the dataframe (the same estructure of this table)
  engine : an Engine, which the Session will use for connection
  method (str): apply rules before insert table. Aviables:
    - 'append': create the table (if not exist)
    - 'replace': drop and recreate the table (old data is erased)
    - 'clean': clean all data with primary key coincide with the df (require implicit primary key or dataframe with tablename_id 				column)
  debug : (bool) if true, show the debug message. Default: False

if you not need apply any mehod, for better opcion, use 'append' method or use insert function 

Returns:
  None
"""
```



### Get data of select into dataframe

```python
table_class.select_to_dataframe(engine=engine)
"""
Get data from engine (for example database) and return a dataframe with the data

Parameters:
	engine : (required) an Engine, which the Session will use for connection
	(others): the parameters for select function

Returns:
	(Dataframe) : of Pandas with the obtained data
"""
```

### Save table into JSON

```python
table_class.save_to_file()
"""
Save a dict value of this class (with getDict) in a file, the route of file is in self.file variable. 
Default path is: .dataframeToDb/TableName.ToDB
"""
```

### Load table into JSON

```python
table_class= Table(name="data_json")
table_class.load_from_file(jsonpath)
"""
Set a table estructure in this class from a file, the file path is save in file param
Parameters:
	path : path of file from load table estructure
"""
```

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

### When launch the 1.X release?
When y tested all function i like for a table, my plans is 0.X i create all function for transport a dataframe to db and work, in the 1.X i tried to create database class who extends the functions for work with Database.

### Cats or Dogs?

Pandas!!!