# from dataframetodb import utils, column
import sqlalchemy
from dataframetodb.utils import tryGet, isTimeFromDatetime, isDateFromDatetime
from dataframetodb.column import Column
from sqlalchemy import Column as sqlCol
from sqlalchemy import Integer

from sqlalchemy import MetaData
from sqlalchemy import Table as sqlTable
from sqlalchemy import select, update, delete, values
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect
import json
import os
import pandas as pd
import datetime as dt
from datetime import datetime
import numpy as np

class Table:
    def __init__(self, **kwargs):
        if str(type(kwargs.get('Df', False)))!="<class 'pandas.core.frame.DataFrame'>":
            raise ValueError("Df: could be <class 'pandas.core.frame.DataFrame'>") 
        #if define 1, not both with XOR operator
        if tryGet(kwargs, 'Df', False, True)==False ^ kwargs.get('Columns', False)==False:
            raise ValueError("Only can define Dataframe or Columns, not both") 

        self.columns=[]
        if tryGet(kwargs, 'Df', False, True):
            self.dataframeToColumns(kwargs)

        if tryGet(kwargs, 'Columns', False, True):
            cols = [isinstance(col, Column) for col in tryGet(kwargs, 'Columns', [])]
            if np.all(cols):
                self.columns = tryGet(kwargs, 'Columns')
            else:
                raise ValueError("A column it not a Column type class") 

        self.name = kwargs.get('Name', "NoName")
        self.file=kwargs.get('File', None)
        self.Base = declarative_base()
        if self.file==None:
            self.file=os.path.join('.dataframeToDb', str(self.name) + ".ToDB")

    def dataframeToColumns(self, kwargs):
        # raise ValueError("Not implemented yet") 
        df = tryGet(kwargs, 'Df')
        qForSample = tryGet(kwargs, 'qForSample', len(df)) #despues aceptar parametro porcentaje, ejemplo 30% de los datos
        if qForSample<0:
            raise ValueError("qForSample: could be positive") 
        if qForSample>len(df):
            print("Warning, qForSample is more than dataframe length, use length instead")
            qForSample=len(df)
        else:
            qForSample = len(df) if len(df)<=100 else int(len(df) * 0.3)

        colList = df.columns.to_list()

        for col in colList:
            exist = [1 for col in self.columns if col==col.colDfName]
            if np.array(exist).sum() > 0:
                print("The column [{}] is already in the table [{}], skip").format(col, self.name)
                continue
            
            custom = tryGet(kwargs, 'Custom')
            type=None
            #si seteamos algo custom de la columna
            if tryGet(custom, col, False, True):
                customCol = tryGet(custom, col)
                if tryGet(customCol, "Type", False, True):
                    type=tryGet(customCol, "Type")
                else:
                    type = self.checkColType(df, col, qForSample, kwargs)
                self.columns.append(
                    Column(
                        colName = tryGet(customCol, "colName", col),
                        colDfName = tryGet(customCol, "colDfName", col),
                        Type = type,
                        PrimaryKey = tryGet(customCol, "PrimaryKey", False),
                        AutoIncrement = tryGet(customCol, "AutoIncrement", False)
                    )
                )
            else:
                type = self.checkColType(df, col, qForSample, kwargs)
                self.columns.append(
                    Column(
                        colName = col,
                        Type = type,
                        colDfName = col,
                        PrimaryKey = False,
                        AutoIncrement = False
                    )
                )

    def getSQLcolumns(self):
        # return [col.colData() for col in self.columns]
        attr_dict={'__tablename__': self.name}
        # attr_dict={}
        for col in self.columns:
            name, coldata = col.colData()
            attr_dict[name] = coldata
        if self.getPrimaryKeys()==[]:
            attr_dict[self.name+"_id"] = sqlCol(self.name+"_id", Integer, primary_key=True)
        return attr_dict

    def getParents(self):
        return [col for col in self.columns if col.fk!=None]

    def getPrimaryKeys(self):
        return [col for col in self.columns if col.primary==True]

    def getTable(self, engine):
        """
        Returns a SqlAlchemy Table instance based in DataframeToDB Table

        Returns:
            (Table) : of SqlAlchemy with the columns of this class
        """
        # meta = MetaData()
        # # meta.reflect(bind=engine)
        # # if self.name in meta.tables: return
        # return type(self.name, meta, self.getSQLcolumns())
        # return sqlTable(self.name, MetaData(), *self.getSQLcolumns())
        # Base = declarative_base()
        return type(self.name, (self.Base,), self.getSQLcolumns())

    def getDict(self):
        """
        Returns a dict in values of this Table class with the columnd dict values

        Returns:
            dict : the values of this class in dict format
        """
        return {
            "Name": self.name, 
            "File": self.file,
            "Type": "Table",
            "Columns": [col.getDict() for col in self.columns]
        }
        # columns = [col.data() for col in self.columns]
        # data["Columns"] = [col.data() for col in self.columns]

    def saveToFile(self):
        """
        Save a dict value of this class (with getDict) in a file, the route 
        of file is in self.file variable. 
        Default path is: .dataframeToDb/TableName.ToDB
        """
        # if not os.path.exists('.dataframeToDb'):
        # The file in the folder not exist
        
        path = os.path.split(self.file)
        if not os.path.exists(self.file):
            #separate file of a path
            #Verify if path exist, if false, create the path
            if os.path.exists(path[0])==False:
                try:
                    os.makedirs(path[0])
                except OSError as e:
                    if e.errno != e.errno.EXIST:
                        raise
        try:

            with open(self.file, 'w') as outfile:
                json.dump(self.getDict(), outfile)
        except ValueError as e:
            print("DataframeToDB: Error save the file - {}".format(e))

    def loadFromFile(self, path):
        if path!=None:
            self.file = path
        data=None
        try:
            data = json.load(self.file)
            self.loadFromJSON(data)
        except:
            print("DataframeToDB: Error reading the file {}".format(path))

    def loadFromJSON(self, json):
        if tryGet(json, "Type")!="Table":
            raise ValueError("DataframeToDB: Error, the data is not a table")
        if tryGet(json, "Type")!="Name":
            raise ValueError("DataframeToDB: Error, the data not have name")
        self.nombre=tryGet(json, "Name")
        self.file=tryGet(json, "File", None)
        self.columns=[]
        for col in tryGet(json, "Columns", []):
            if tryGet(col, "col_name", False)==False:
                print("DataframeToDB: Error, the row of table not have col_name")
                return False
            if tryGet(col, "Type", False)==False:
                print("DataframeToDB: Error, the row of table not have Type")
                return False
            self.columns.append(
                Column(
                    {
                        "col_name": tryGet(col, "col_name"),
                        "col_dfName": tryGet(col, "col_dfName", tryGet(col, "col_name")),
                        "Type": tryGet(col, "Type"),
                        "primary": tryGet(col, "Primary Key", False),
                        "Auto Increment": tryGet(col, "Auto Increment", False)
                    }
                )
            )

    # def checkColType(self, df, col, qForSample, kwargs):
    def checkColType(self, df, col, qForSample, kwargs):

        if str(df.dtypes[col])=="Int64" or str(df.dtypes[col])=="int64":
            if df[col].sample(qForSample).apply(lambda x: x<=-2147483648 and x>=2147483647 ).all().item(): #corregir
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Integer, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))
                return "Integer"
            else:
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: BigInteger, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))
                return "BigInteger"

        elif str(df.dtypes[col])=="float64":
            if kwargs.get('debug', False):
                print("Nombre: {}, Tipo: {}, ColType: Float, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))
            return "Float"
        elif str(df.dtypes[col])=="boolean":
            if kwargs.get('debug', False):
                print("Nombre: {}, Tipo: {}, ColType: Boolean, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))
            return "Boolean"
        elif str(df.dtypes[col])=="string":
            if df[col].sample(qForSample).apply(lambda x: len(str(x))<=255).all().item():
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: String, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
                return "String"
            else:
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Text, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
                return "Text"
        elif str(df.dtypes[col])=='datetime64[ns]': #si es date time, puede ser datetime, date or time
            # sample = pd.Timestamp(df[col].sample(1).values[0])
            if df[col].sample(qForSample).apply(lambda x: isDateFromDatetime(x)).all().item():#sample==dt.time(hour=sample.hour, minute=sample.minute, second=sample.microsecond, microsecond=sample.microsecond):#si es time
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Date".format(col, str(df.dtypes[col])))
                return "Date"
            elif df[col].sample(qForSample).apply(lambda x: isTimeFromDatetime(x)).all().item():# sample==dt.date(year=sample.year, month=sample.month, day=sample.day):
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Time".format(col, str(df.dtypes[col])))
                return "Time"
            else: #dt.datetime(year=sample.year, month=sample.month, day=sample.day, hour=sample.hour, minute=sample.minute, second=sample.microsecond, microsecond=sample.microsecond)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: DateTime".format(col, str(df.dtypes[col])))
                return "DateTime"
        else:
            raise ValueError("Not suported, Name Df: {}, dtype: {}".format(col, str(df.dtypes[col])))

    def insert(self, df, engine, debug=False):
        """
        Insert data of dataframe into database (is necesary conection),
        if any error appears in the dataframe insert, apply rollback

        Parameters:
            df : the dataframe (the same estructure of this table)
            engine : an Engine, which the Session will use for connection

        Returns:
            (Table) : of SqlAlchemy with the columns of this class
        """
        tbl = self.getTable()
        with Session(engine) as session:
            session.begin()
            if debug:
                print("starting to save the data in the selected database, you can pray that it does not fail in the meantime")
            try:
                for index, row in df.iterrows():
                    newRow = tbl(**row.to_dict())
                    session.add(newRow)
            except Exception as e:
                session.rollback()
                raise ValueError("Error trying insert a element of dataframe, apply rollback, Erroe message [{}]".format(e)) 
            else:
                session.commit()

    def toDb(self, df, engine, method='append', debug=False):
        """
        Insert data of dataframe into database (is necesary conection),
        and apply method for try create database
        Use insert function for add data to db

        Parameters:
            df : the dataframe (the same estructure of this table)
            engine : an Engine, which the Session will use for connection
            method (str): apply rules before insert table. Aviables:
                - 'append': create the table (if not exist)
                - 'replace': drop and recreate the table (old data is erased)
                - 'clean': clean all data with primary key coincide with the df (require implicit primary key or dataframe with tablename_id column)
            
        if you not need apply any mehod, for better opcion, use 'append' method or
        use insert function 

        Returns:
            None
        """
        
        tbl = self.getTable(engine)
        with Session(engine) as session:
            session.begin()
            try:
                if method=="append":
                    # Base = declarative_base()
                    # Base.metadata.create_all(engine)
                    self.Base.metadata.create_all(engine, checkfirst=True)
                    # self.Base.metadata.create_all(engine, tables=tbl, checkfirst=True)
                    # tbl.create(engine, checkfirst=True)
                if method=="replace":
                    self.Base.metadata.drop_all(engine, checkfirst=True)
                    self.Base.metadata.create_all(engine, checkfirst=False)
                if method=="clean":
                    # get name of primary keys cols
                    pkcols = [col.colDfName for col in self.getPrimaryKeys()]
                    if pkcols==[] and not(self.name + "_id" in df.columns): #revisa si la tabla tiene primary key por clase o construccion
                        raise ValueError("Error, for clean method you need one primary key implicit at least, if use autogenerate, you need a column in dataframe with name '{}'".format(self.name + "_id")) 
                    self.Base.metadata.create_all(engine, checkfirst=True)
                    # drop duplicates primary key for dataframe
                    dfTemp = df.drop_duplicates(subset=pkcols)
                    # drop any coincidence of dataframe cleaned
                    for index, row in dfTemp.iterrows():
                        filters = row.to_dict()
                        tbl.query.filter_by(**filters).delete()
            except Exception as e:
                session.rollback()
                raise ValueError("Error trying insert a element of dataframe, apply rollback, Erroe message [{}]".format(e)) 
            else:
                session.commit()
        

