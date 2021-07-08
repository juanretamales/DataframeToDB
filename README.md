# DataframeToDB

_____

This project is a personal non-profit project rather than accepting donations to buy more coffee and carry out more open source projects! 

If you want to donate:  https://ko-fi.com/juanretamales

## Introduction

`DataframeToDB` is an improved way to upload pandas dataframes to Microsoft SQL Server, MySQL, PostgreDB and support for other type of database.

`DataframeToDB` takes advantage of SQLAlchemy. This allows for a much lighter weight import for writing pandas dataframes to db server. 

When to use:

- When you need save dataframes, for example when scraping many tables
- You need shared a database estructure for use in proyects
- When you need save multi excels (ported to dataframed) in a database
- When you consider use FastAPI (With the SQLAlchemy compatibility) (Work in progress)
- When you need create a estructure of table for any reason


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

Pandas!!! ...ba dum tss...