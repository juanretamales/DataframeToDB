# **DataframeToDB** Documentation

This is a simple documentation for a simple library.

This project is a personal non-profit project rather than accepting donations to buy more coffee and carry out more open source projects! 

If you want to donate: https://ko-fi.com/juanretamales

## Requirements

- Written for Python 3.8+
- numpy>=1.20.2
- pandas>=1.2.4
- python-dateutil>=2.8.1
- SQLAlchemy>=1.4.9
- **driver for db conection**

## Whats is DataframeToDB?

`DataframeToDB` is an improved way to upload pandas dataframes to Microsoft SQL Server, MySQL, PostgreDB and support for other type of database.

`DataframeToDB` takes advantage of SQLAlchemy. This allows for a much lighter weight import for writing pandas dataframes to db server. 

## How to work

The Table of DataframeToDB is a custom class who generate a SQLAlchemy Table when you call getTable function.

## When to use

- When you need save dataframes, for example when scraping a table
- You need shared a database estructure for use in proyects
- When you need save multi excels (ported to dataframed)

## Quick start

In the following lines, what has been done in the functions will be explained with comments to quickly understand the operation.

```python
"""First we need import the class to work"""
import pandas as pd
import dataframetodb
from dataframetodb import Table, refactor
from datetime import datetime
import os

"""Now we set the name of table"""
nametable = "nameTable"
"""We need create a engine, the function is the same of sqlalchemy but for ease it can be used as shown in the example. In this sample example we will use sqlite using the previously saved table name."""
engine = dataframetodb.create_engine('sqlite:///{}.sqlite'.format(nametable))
"""Now we must get the data in the form of dataframe, so we will use the pandas function called read_csv"""
df = pd.read_csv("./dataset/data.csv")
"""With the refactor function change the types of variables of the dataframe, although this step is optional and you can do it manually."""
df = refactor(df) 
"""Now that we have a dataframe ready to send to the database but we must first create the Table instance"""
table_class = Table(name=nametable, df=df)
"""We already have the instance table, and we can now send the dataframe to the database, the load_from_file function can be used to load a Table instance in future operations for this same dataframe"""
table_class.toDb(df, engine, 'append')
```



## DataframeToDB.Table

Here describe the functions of a Table class with examples. A Table has many columns, but for initialize the class you need only a name who table has in database.



### Table class

`mytable = Table(Name=nametable, df=df, File=None)`

The use of certain variables (custom) are only used to call the mytable.dataframe_to_columns function.

The use of file is only used to call the mytable.save or mytable.load_from_file (if not send any path) functions.

- **name **(str): (required) Name of table for saved in Database
- **df** (Dataframe): (excluyent*) The Dataframe who you like get the Table estructure
- **columns** (List): (excluyent*) A list of DataframeToDB.Columns class
- **file** (str): (optional) you can set a file for save the estructure for future use
- **q_sample** (int): (optional) Number of elements of the DataFrame to determine the type (only use if your define df param)
- **custom** (dict): (optional) a dict who use for determinate de column class, if use this, you need a estructure how the next example with the column name in the index:
  - **col_name** (str): (required) Name of column for saved in Table
  - **col_df_name** (str): (optional) The column name of Dataframe who you like get the column estructure
  - **type** (str): (required) type of column
  - **primary_key** (bool): (optional) you can set if the column is primary key
  - **nullable** (bool): (optional) you can set if this column can has a null values

Note: required is necesary, excluyent* is mutualy excluyent, you can use only one or none

### mytable.dataframe_to_columns

```python
mytable.dataframe_to_columns(df=df)
```

Set Columns from a dataframe df. 

Note: Custom parameter has priority from mytable.check_col_type

Parameters:

- **df** (Dataframe): (excluyent*) The Dataframe who you like get the Table estructure

- **q_sample** (int): (optional) Number of elements of the DataFrame to determine the type (only use if your define df param)
- **custom** (dict): (excluyent*) a dict who use for determinate de column class, if use this, you need a estructure how the next example with the column name in the index:
  - **col_name** (str): (required) Name of column for saved in Table
  - **col_df_name** (str): (optional) The column name of Dataframe who you like get the column estructure
  - **type** (str): (required) type of column
  - **primary_key** (bool): (optional) you can set if the column is primary key
  - **nullable** (bool): (optional) you can set if this column can has a null values

### mytable.get_dict_columns

```python
mytable.get_dict_columns()
```

Returns a dict for create SQLAlchemy table

Returns:

- (dict) : dict for create SQLAlchemy table

### mytable.get_parents

```
mytable.get_parents()
```

Returns a list with columns who contains foreign key

Returns:

- (List) : list with columns who contains foreign key

### mytable.get_primary_keys

```
mytable.get_primary_keys()
```

Returns a list with columns with primary key

Returns:

- (List) : list with columns with primary key

### mytable.get_table

```
mytable.get_table(engine)
```

Returns a SqlAlchemy Table instance based in DataframeToDB Table

Parameters:

- engine : (required) a SQLAlchemy engine

Returns:

- (Table) : of SqlAlchemy with the columns of this class

### mytable.save_to_file

```
mytable.save_to_file()
```

Save a dict value of this class (with getDict) in a file, the route of file is in self.file variable. 

Default path is: .dataframeToDb/TableName.ToDB

### mytable.load_from_file

```
mytable.load_from_file("./table.ToDB")
```

Set a table estructure in this class from a file, the file path is save in file param

Parameters:

- path (str) : (optional) path of file from load table estructure

### mytable.load_from_dict

```
mytable.load_from_dict(dict)
```

Set a table estructure in this class from a dict, usually of a json generate of a save function

Parameters:

- dict (dict) : (required) dict with the structure of table

The structure of the dict is similar to the following:

```python
{
    "name": "mytable", #required
    "file": ".dataframeToDb\\mytable.ToDB", 
    "type": "table", #required
    "columns": [
        {
            "col_name": "col_name", #required
            "col_df_name": "id", 
            "type": "BigInteger", #required
            "primary_key": True, 
            "auto_increment": False,
            "nullable": False
        }, ...
    ]
}
```

The data with required is because if not appears the function raise error.

### mytable.check_col_type

```python
mytable.check_col_type()
```

Return a name of column type of SQLalchemy from examinated sample

Parameters:

- df (dataframe): (required) The Dataframe who you like get the Table estructure
- col (str): (required) Column name for analize
- q_sample (int): (optional) Number of elements of the DataFrame to determine the type (only use if your define df param)

Returns:

- (str) : name of SqlAlchemy column type

### mytable.execute

```
mytable.execute()
```

Execute a query with the engine and return the results

Parameters:

- engine : (required) an Engine, which the Session will use for connection

- query : (required) a sqlalchemy query

Returns:

- (results) : of SqlAlchemy query executed



### Others

Functions are missing, but they are still in development or I have not had time to document them correctly yet

## DataframeToDB.Column

Here describe the functions of a Column class with examples.  Tt is generally used automatically with the Table class, but I leave the functions for advanced operations below. 

### Column class

`mycolumn = Column(col_name=col_name, col_df_name=col_df_name, File=None)`

- **col_name** (str): (required) Name of column for saved in Table
- **col_df_name** (str): (optional) The column name of Dataframe who you like get the column estructure
- **type** (str): (required) type of column, the types available are:
  - Integer
  - BigInteger
  - SmallInteger
  - String
  - Text
  - Date
  - Time
  - DateTime
  - Float
  - Enum
  - Boolean
  - Interval
  - LargeBinary
  - PickleType
  - Unicode
  - UnicodeText
- **primary_key** (bool): (optional) you can set if the column is primary key
- **auto_increment** (bool): (optional) you can set if this column is auto increment (only for Ingeger and BigInteger type)
- **nullable** (bool): (optional) you can set if this column can has a null values
- **fk**: (not implemented yet) is for create a ForeignKey
- **rs**: (not implemented yet) is for create a relationship("Child")

Note: required is necesary, excluyent* is mutualy excluyent, you can use only one or none

### validType

```python
mycolumn.validType("String")
```

Returns a a SQL class type based in a match with the text and a list dict

Parameters:
	text : name of sql type

Returns:
	(sql type class) or False : SQL type or False if not found

### get_dict

```python
mycolumn.get_dict()
```

Returns a dict in values of this Table class with the columnd dict values

Returns:
	dict : the values of this class in dict format

