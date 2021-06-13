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

## DataframeToDB.Table

Here describe the functions of a Table class with examples.

### Table class

`mytable = Table(Name=nametable, Df=df, File=None)`

- **Name**: (required) Name of table
- **Df**: (excluyent*) The Dataframe who you like get the Table estructure
- **Columns**: (excluyent*) A list of DataframeToDB.Columns class
- **File**: you can set a file for save the estructure for future use

Note: required is necesary, excluyent* is mutualy excluyent, you can use only one or none