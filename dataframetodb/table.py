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
        """
        name: (required) Name of table for saved in Database
        df: (excluyent*) The Dataframe who you like get the Table estructure
        columns: (excluyent*) A list of DataframeToDB.Columns class
        file: You can set a file for save the estructure for future use
        q_sample: Number of elements of the DataFrame to determine the type (only use if your define df param)
        custom: a dict who use for determinate de column class, if use this, you need a estructure how the next example:
            col_name: Name of column for saved in Table
            col_df_name: Name of column of dataframe
            type:
            primary_key:
            auto_increment:



        Note: required is necesary, excluyent* is mutualy excluyent, you can use only one or none
        """
        if str(type(kwargs.get('df', False)))!="<class 'pandas.core.frame.DataFrame'>":
            raise ValueError("df: could be <class 'pandas.core.frame.DataFrame'>") 
        #if define 1, not both with XOR operator
        if tryGet(kwargs, 'df', False, True)==False ^ kwargs.get('columns', False)==False:
            raise ValueError("Only can define Dataframe or Columns, not both") 

        self.columns=[]
        if tryGet(kwargs, 'df', False, True):
            self.dataframe_to_columns(kwargs)

        if tryGet(kwargs, 'columns', False, True):
            cols = [isinstance(col, Column) for col in tryGet(kwargs, 'columns', [])]
            if np.all(cols):
                self.columns = tryGet(kwargs, 'columns')
            else:
                raise ValueError("A column it not a Column type class") 

        self.name = kwargs.get('name', "NoName")
        self.file=kwargs.get('file', None)
        self.Base = declarative_base()
        if self.file==None:
            self.file=os.path.join('.dataframeToDb', str(self.name) + ".ToDB")

    def dataframe_to_columns(self, kwargs):
        df = tryGet(kwargs, 'df')
        qForSample = tryGet(kwargs, 'q_sample', len(df)) #despues aceptar parametro porcentaje, ejemplo 30% de los datos
        if qForSample<0:
            raise ValueError("q_sample: could be positive") 
        if qForSample>len(df):
            print("Warning, q_sample is more than dataframe length, use length instead")
            qForSample=len(df)
        else:
            qForSample = len(df) if len(df)<=100 else int(len(df) * 0.3)

        colList = df.columns.to_list()

        for col in colList:
            exist = [1 for col in self.columns if col==col.col_df_name]
            if np.array(exist).sum() > 0:
                print("The column [{}] is already in the table [{}], skip").format(col, self.name)
                continue
            
            custom = tryGet(kwargs, 'Custom')
            type=None
            #si seteamos algo custom de la columna
            if tryGet(custom, col, False, True):
                customCol = tryGet(custom, col)
                if tryGet(customCol, "type", False, True):
                    type=tryGet(customCol, "type")
                else:
                    type = self.checkColType(df, col, qForSample, kwargs)
                self.columns.append(
                    Column(
                        col_name = tryGet(customCol, "col_name", col),
                        col_df_name = tryGet(customCol, "col_df_name", col),
                        type = type,
                        primary_key = tryGet(customCol, "primary_key", False),
                        auto_increment = tryGet(customCol, "auto_increment", False),
                        nullable = tryGet(customCol, "nullable", False)
                    )
                )
            else:
                type = self.check_col_type(df, col, qForSample, kwargs)
                self.columns.append(
                    Column(
                        col_name = col,
                        type = type,
                        col_df_name = col,
                        primary_key = False,
                        auto_increment = False,
                        nullable = False
                    )
                )

    def get_dict_columns(self):
        """
        Returns a dict for create SQLAlchemy table

        Returns:
            (dict) : dict for create SQLAlchemy table
        """
        attr_dict={'__tablename__': self.name}
        for col in self.columns:
            name, coldata = col.col_data()
            attr_dict[name] = coldata
        if self.get_primary_keys()==[]:
            attr_dict[self.name+"_id"] = sqlCol(self.name+"_id", Integer, primary_key=True, autoincrement=True)
        return attr_dict

    def get_parents(self):
        """
        Returns a list with columns who contains foreign key

        Returns:
            (List) : list with columns who contains foreign key
        """
        return [col for col in self.columns if col.fk!=None]

    def get_primary_keys(self):
        """
        Returns a list with columns with primary key

        Returns:
            (List) : list with columns with primary key
        """
        return [col for col in self.columns if col.primary==True]

    def get_table(self, engine):
        """
        Returns a SqlAlchemy Table instance based in DataframeToDB Table

        Returns:
            (Table) : of SqlAlchemy with the columns of this class
        """
        try: #revisa si tiene la tabla ya agregada a la Base y retorna esa en vez de crearla
            if self.name in self.Base.metadata.tables.keys():
                return self.Base.metadata.tables[self.name]
        except:
            pass
        return type(self.name, (self.Base,), self.get_dict_columns())

    def get_dict(self):
        """
        Returns a dict in values of this Table class with the columnd dict values

        Returns:
            dict : the values of this class in dict format
        """
        return {
            "Name": self.name, 
            "File": self.file,
            "Type": "Table",
            "Columns": [col.get_dict() for col in self.columns]
        }
        # columns = [col.data() for col in self.columns]
        # data["Columns"] = [col.data() for col in self.columns]

    def save_to_file(self):
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

    def load_from_file(self, path):
        if path!=None:
            self.file = path
        data=None
        try:
            data = json.load(self.file)
            self.loadFromJSON(data)
        except:
            print("DataframeToDB: Error reading the file {}".format(path))

    def load_from_JSON(self, json):
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

    def check_col_type(self, df, col, qForSample, kwargs):

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

    def execute(self, engine, query):
        """
        Get data from engine (for example database) and return a dataframe with the data

        Parameters:
            engine : (required) an Engine, which the Session will use for connection
            query : a sqlalchemy query

        Returns:
            (results) : of SqlAlchemy query executed
        """
        connection = engine.connect()
        query = sqlalchemy.select([self.get_table(engine)])
        results=connection.execute(query)
        # if query.is_insert or query.is_delete or query.is_update():

        # if query.is_select:
        #     return results.fetchall()
        return results

    def select(self, engine, filter_by=None):
        """
        Get data from engine (for example database) and return a list with the data

        Parameters:
            engine : (required) an Engine, which the Session will use for connection
            filter_by : a dict with the filters apply to select query, for example {"name":"evans"}

        Returns:
            (List) : with the obtained data
        """
        if filter!=None:
            if not isinstance(filter_by, dict):
                raise ValueError("Error, filter is not dict.") 
            query = sqlalchemy.select([self.get_table(engine)]).filter_by(**filter)
        else:
            query = sqlalchemy.select([self.get_table(engine)])
        try:
            execute = self.execute(engine, query)
            return execute.fetchall()
        except Exception as e:
            raise ValueError("Error trying insert a element of dataframe, apply rollback, Erroe message [{}]".format(e)) 

    def select_to_dataframe(self, engine, filter_by=None):
        """
        Get data from engine (for example database) and return a dataframe with the data

        Parameters:
            engine : (required) an Engine, which the Session will use for connection
            filter_by : a dict with the filters apply to select query, for example {"name":"evans"}

        Returns:
            (Dataframe) : of Pandas with the obtained data
        """
        cols = [col.col_name for col in self.columns]
        if len(self.get_primary_keys)==0:
            cols.append(self.name+"_id")
            df = pd.DataFrame(self.select(engine, filter_by), columns=cols)
            return df[cols[:-1]]
        return pd.DataFrame(self.select(engine, filter_by), columns=cols)

    def insert(self, data, engine, debug=False):
        """
        Insert data of dict into database (is necesary conection),
        if any error appears in the dataframe insert, apply rollback

        Parameters:
            data : (required) the dict (the same estructure of this table)
            engine : (required) an Engine, which the Session will use for connection

        Returns:
            (Table) : of SqlAlchemy with the columns of this class
        """
        if not isinstance(data, dict):
            raise ValueError("Error, data is not dict.") 
        tbl = self.get_table(engine)
        with Session(engine) as session:
            session.begin()
            if debug:
                print("starting to save the data in the selected database, you can pray that it does not fail in the meantime")
            try:
                newRow = tbl.insert().values(**data)
                session.execute(newRow) 
            except Exception as e:
                session.rollback()
                raise ValueError("Error trying insert a element of dataframe, apply rollback, Erroe message [{}]".format(e)) 
            session.commit()

    def dataframe_insert(self, df, engine, debug=False):
        """
        Insert data of dataframe into database (is necesary conection),
        if any error appears in the dataframe insert, apply rollback

        Parameters:
            data : the dataframe (the same estructure of this table)
            engine : an Engine, which the Session will use for connection

        Returns:
            (Table) : of SqlAlchemy with the columns of this class
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Error, df is not dataframe.") 
        tbl = self.get_table(engine)
        with Session(engine) as session:
            session.begin()
            if debug:
                print("starting to save the data in the selected database, you can pray that it does not fail in the meantime")
            try:
                for index, row in df.iterrows():
                    newRow = tbl.insert().values(**row.to_dict())
                    session.execute(newRow) 
            except Exception as e:
                session.rollback()
                raise ValueError("Error trying insert a element of dataframe, apply rollback, Erroe message [{}]".format(e)) 
            session.commit()


    def clean(self, df, engine, debug=False):
        """
        Clean data with primary key into database (is necesary conection),
        if any error appears in the dataframe insert, apply rollback

        Parameters:
            df : the dataframe (the same estructure of this table)
            engine : an Engine, which the Session will use for connection
            session : a session, if not apears, create a new session

        Returns:
            (Table) : of SqlAlchemy with the columns of this class
        """
        tbl = self.get_table(engine)
        with Session(engine) as session:
            session.begin()
            try:
                # get name of primary keys cols
                pkcols = [col.col_df_name for col in self.get_primary_keys()]
                if pkcols==[] and not(self.name + "_id" in df.columns): #revisa si la tabla tiene primary key por clase o construccion
                    raise ValueError("Error, for clean method you need one primary key implicit at least, if use autogenerate, you need a column in dataframe with name '{}'".format(self.name + "_id")) 
                # self.Base.metadata.create_all(engine, checkfirst=True)
                self.Base.metadata.tables[self.name].create(engine, checkfirst=True)
                # drop duplicates primary key for dataframe
                dfTemp = df.drop_duplicates(subset=pkcols)
                pk = self.get_primary_keys() #id of primarykeys
                if len(pkcols)==1:
                    idpk=[]
                    if str(df.dtypes[pkcols[0]])=="string":
                        idpk = np.unique(["'{}'".format(i) for i in df[pkcols[0]]])
                    else:
                        idpk = np.unique([i for i in df[pkcols[0]]])
                    # drop any coincidence of dataframe cleaned
                    #stmt = Users.__table__.delete().where(Users.id.in_(subquery...))
                    # session.query(tbl).filter(tbl.pk.in_(idpk)).delete()
                    session.query(tbl).filter(tbl[pk].in_(idpk)).delete()
                else:
                    df[pkcols]
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
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Error, df is not dataframe.") 
        tbl = self.get_table(engine)
        with Session(engine) as session:
            session.begin()
            try:
                if method=="append":
                    self.Base.metadata.tables[self.name].create(engine, checkfirst=True)
                if method=="replace":
                    self.Base.metadata.tables[self.name].drop(engine, checkfirst=True) 
                    self.Base.metadata.tables[self.name].create(engine, checkfirst=True)
                if method=="clean":
                   self.clean(df, engine)

                self.dataframe_insert(df, engine, debug)
            except Exception as e:
                session.rollback()
                raise ValueError("Error trying insert a element of dataframe, apply rollback, Error message [{}]".format(e)) 
            else:
                session.commit()
            session.commit()
        

