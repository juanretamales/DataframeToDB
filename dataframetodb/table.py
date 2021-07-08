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
        name (str): (required) Name of table for saved in Database
        df (Dataframe): (excluyent*) The Dataframe who you like get the Table estructure
        columns (List): (excluyent*) A list of DataframeToDB.Columns class
        file (str): You can set a file for save the estructure for future use
        q_sample (int): Number of elements of the DataFrame to determine the type (only use if your define df param)
        custom (dict): a dict who use for determinate de column class, if use this, you need a estructure how the next example with the column name in the index:
            col_name (str): (required) Name of column for saved in Table
            col_df_name (str): (optional) The column name of Dataframe who you like get the column estructure
            type (str): (required)  type of column
            primary_key (bool): (optional) you can set if the column is primary key
            auto_increment (bool): (optional) you can set if this column is auto increment (only for Ingeger and BigInteger type)
            nullable (bool): (optional)  you can set if this column can has a null values
            
        Note: required is necesary, excluyent* is mutualy excluyent, you can use only one or none
        """
        
        #if define 1 or none, not both with XOR operator
        # if tryGet(kwargs, 'df', False, True)==False ^ kwargs.get('columns', False)==False:
        if tryGet(kwargs, 'df', False, True) and tryGet(kwargs, 'columns', False, True):
            raise ValueError("Only can define Dataframe or Columns, not both") 

        self.columns=[]
        if tryGet(kwargs, 'df', False, True):
            if not isinstance(kwargs.get('df', False), pd.DataFrame):
                raise ValueError("df: could be <class 'pandas.core.frame.DataFrame'>") 
            self.dataframe_to_columns(**kwargs)

        if tryGet(kwargs, 'columns', False, True):
            cols = [isinstance(col, Column) for col in tryGet(kwargs, 'columns', [])]
            if np.all(cols):
                self.columns = tryGet(kwargs, 'columns')
            else:
                raise ValueError("A column it not a Column type class") 

        self.name = tryGet(kwargs, 'name', "NoName")
        self.file = tryGet(kwargs, 'file', None)
        self.Base = declarative_base()
        if self.file==None:
            self.file=os.path.join('.dataframeToDb', str(self.name) + ".ToDB")

    def dataframe_to_columns(self, **kwargs):
        """
        Set Columns from a dataframe df

        Parameters:
            df : (required) Dataframe
            q_sample: Number of elements of the DataFrame to determine the type (only use if your define df param)
            custom (dict): (excluyent*) a dict who use for determinate de column class, if use this, you need a estructure how the next example with the column name in the index:
                col_name (str): (required) Name of column for saved in Table
                col_df_name (str): (optional) The column name of Dataframe who you like get the column estructure
                type (str): (required)  type of column
                primary_key (bool): (optional) you can set if the column is primary key
                auto_increment (bool): (optional) you can set if this column is auto increment (only for Ingeger and BigInteger type)
                nullable (bool): (optional)  you can set if this column can has a null values
        """
        df = tryGet(kwargs, 'df')
        q_sample = tryGet(kwargs, 'q_sample', len(df)) #despues aceptar parametro porcentaje, ejemplo 30% de los datos
        if q_sample<0:
            raise ValueError("q_sample: could be positive") 
        if q_sample>len(df):
            print("Warning, q_sample is more than dataframe length, use length instead")
            q_sample=len(df)
        else:
            q_sample = len(df) if len(df)<=100 else int(len(df) * 0.3)

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
                    kwargs["col"]=col
                    kwargs["q_sample"]=q_sample
                    type = self.checkColType(**kwargs)
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
                kwargs["col"]=col
                kwargs["q_sample"]=q_sample
                type = self.check_col_type(**kwargs)
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

        Parameters:
            engine : (required) a SQLAlchemy engine

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
            "name": self.name, 
            "file": self.file,
            "type": "table",
            "columns": [col.get_dict() for col in self.columns]
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
                json.dump(self.get_dict(), outfile)
        except ValueError as e:
            print("DataframeToDB: Error save the file - {}".format(e))

    def load_from_file(self, path=None):
        """
        Set a table estructure in this class from a file, the file path is save in file param

        Parameters:
            path (str) : (optional) path of file from load table estructure
        """
        if path!=None:
            self.file = path
        data=None
        try:
            data={}
            with open(self.file) as f:
                data = json.load(f)
            self.load_from_dict(data)
        except ValueError as e:
            raise ValueError("DataframeToDB: Error reading the file {}, message [{}]".format(path, e))

    def load_from_dict(self, json):
        """
        Set a table estructure in this class from a dict, usually of a json generate of a save function

        Parameters:
            json (dict) : (required) dict with the structure of table
        """
        if tryGet(json, "type")!="table":
            raise ValueError("DataframeToDB: Error, the data is not a table")
        if tryGet(json, "name", True, False):
            raise ValueError("DataframeToDB: Error, the data not have name")
        self.nombre=tryGet(json, "name")
        self.file=tryGet(json, "file", None)
        self.columns=[]
        for col in tryGet(json, "columns", []):
            if tryGet(col, "col_name", True, False):
                raise ValueError("DataframeToDB: Error, the row of table not have col_name")
            if tryGet(col, "type", True, False):
                raise ValueError("DataframeToDB: Error, the row of table not have Type")
            self.columns.append(
                Column(
                    col_name= tryGet(col, "col_name"),
                    col_df_name= tryGet(col, "col_df_name", tryGet(col, "col_name")),
                    type= tryGet(col, "type"),
                    primary= tryGet(col, "primary_key", False),
                    auto_increment= tryGet(col, "auto_increment", False)
                )
            )

    def load_from_db(self, engine, ignore_error=False):
        _meta = MetaData(bind=engine, reflect=True)
        if tryGet(_meta.tables, self.name, False, True):
            print("Table exists")
        else:
            if ignore_error==False:
                raise ValueError("DataframeToDB: Error, the table not exist in db, test with ignore_error=True")
            else:
                print("DataframeToDB: Error, the table '[{}]' not exist in db, check database".format(self.name))

    def check_col_type(self, **kwargs):
        """
        Return a name of column type of SQLalchemy from examinated sample

        Parameters:
            df (dataframe): (required) The Dataframe who you like get the Table estructure
            col (str): (required) Column name for analize
            q_sample (int): (optional) Number of elements of the DataFrame to determine the type (only use if your define df param)

        Returns:
            (str) : name of SqlAlchemy column type
        """
        if tryGet(kwargs, "df", True, False):
            raise ValueError("Error, df is required.") 
        df = tryGet(kwargs, "df")
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Error, df is not dataframe.") 
        if tryGet(kwargs, "col", True, False):
            raise ValueError("Error, col is required.") 
        col = tryGet(kwargs, "col")
        q_sample = int(tryGet(kwargs, "q_sample", len(df)))
        if str(df.dtypes[col])=="Int64" or str(df.dtypes[col])=="int64":
            if df[col].sample(q_sample).apply(lambda x: x<=-2147483648 and x>=2147483647 ).all().item(): #corregir
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
            if df[col].sample(q_sample).apply(lambda x: len(str(x))<=255).all().item():
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: String, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
                return "String"
            else:
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Text, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
                return "Text"
        elif str(df.dtypes[col])=='datetime64[ns]': #si es date time, puede ser datetime, date or time
            # sample = pd.Timestamp(df[col].sample(1).values[0])
            if df[col].sample(q_sample).apply(lambda x: isDateFromDatetime(x)).all().item():#sample==dt.time(hour=sample.hour, minute=sample.minute, second=sample.microsecond, microsecond=sample.microsecond):#si es time
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Date".format(col, str(df.dtypes[col])))
                return "Date"
            elif df[col].sample(q_sample).apply(lambda x: isTimeFromDatetime(x)).all().item():# sample==dt.date(year=sample.year, month=sample.month, day=sample.day):
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
        Execute a query with the engine and return the results

        Parameters:
            engine : (required) an Engine, which the Session will use for connection
            query : (required) a sqlalchemy query

        Returns:
            (results) : of SqlAlchemy query executed
        """
        connection = engine.connect()
        if isinstance(query, str):
            query=sqlalchemy.text(query)
        results=connection.execute(query)
        # if query.is_insert or query.is_delete or query.is_update():

        # if query.is_select:
        #     return results.fetchall()
        return results

    def select(self, **kwargs):
        """
        Get data from engine (for example database) and return a list with the data

        Parameters:
            engine : (required) an Engine, which the Session will use for connection
            filter: (excluyent*) a dict with the filters apply to select query, for example {"name":"evans"}
            params: params for filter params if you use :value
            filter_by : (excluyent*) a dict with the filters apply to select query, for example {"name":"evans"}

        Returns:
            (List) : with the obtained data
        """
        if tryGet(kwargs, "engine", True, False):
            raise ValueError("Error trying select data, engine is necesary") 
        engine = tryGet(kwargs, "engine")
        if tryGet(kwargs, "filter", False, True):
            filter=tryGet(kwargs, "filter")
            if ":" in filter and tryGet(kwargs, "params", True, False):
                raise ValueError("if use params, you need set params value")
            param=tryGet(kwargs, "params")
            if param!=False:
                query = sqlalchemy.select([self.get_table(engine)]).filter(sqlalchemy.text(filter)).params(**param)
            else:
                query = sqlalchemy.select([self.get_table(engine)]).filter(sqlalchemy.text(filter))

        elif tryGet(kwargs, "filter_by", False, True):
            query = sqlalchemy.select([self.get_table(engine)]).filter_by(tryGet(kwargs, "filter_by"))
        else:
            query = sqlalchemy.select([self.get_table(engine)])

        try:
            execute = self.execute(engine, query)
            return execute.fetchall()
        except Exception as e:
            raise ValueError("Error trying insert a element of dataframe, apply rollback, Erroe message [{}]".format(e)) 

    def select_to_dataframe(self, **kwargs):
        """
        Get data from engine (for example database) and return a dataframe with the data

        Parameters:
            engine : (required) an Engine, which the Session will use for connection
            filter_by : a dict with the filters apply to select query, for example {"name":"evans"}

        Returns:
            (Dataframe) : of Pandas with the obtained data
        """
        ResultSet = self.select(**kwargs)
        df = pd.DataFrame(ResultSet, columns=ResultSet[0].keys())
        if len(self.get_primary_keys())==0:
            cols=[col for col in ResultSet[0].keys() if col!=self.name+"_id"]
            return df[cols]
        return df

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
            df (dataframe) : (required) the dataframe (the same estructure of this table)
            engine : (required) an Engine, which the Session will use for connection

        Returns:
            (array) : return an array of results of inserts
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Error, df is not dataframe.") 
        tbl = self.get_table(engine)
        results = []
        with Session(engine) as session:
            session.begin()
            if debug:
                print("starting to save the data in the selected database, you can pray that it does not fail in the meantime")
            try:
                for index, row in df.iterrows():
                    newRow = tbl.insert().values(**row.to_dict())
                    results.append(session.execute(newRow) )
            except Exception as e:
                session.rollback()
                results.append("Error trying insert a element of dataframe, apply rollback, Error message [{}]".format(e))
                raise ValueError("Error trying insert a element of dataframe, apply rollback, Error message [{}]".format(e)) 
            session.commit()
        return results

    def delete(self, data, engine, debug=False):
        """
        Delete data with primary key into database (is necesary conection),
        if any error appears in the operation, apply rollback

        Parameters:
            data (dict) : (required) dict for filter the data. ex "id=3"
            engine : (required) an Engine, which the Session will use for connection
        Returns:
            (object) : return of query for delete elements of database
        """
        result=None
        tbl = self.get_table(engine)
        with Session(engine) as session:
            session.begin()
            try:
                delRow = session.query(tbl).filter(**data).delete() #filter and delete for the cols
                result=session.execute(delRow) 
            except Exception as e:
                session.rollback()
                raise ValueError("Error trying Delete a element of dataframe, apply rollback, Error message [{}]".format(e)) 
            session.commit()
        return result


    def clean(self, df, engine, debug=False):
        """
        Clean data with primary key into database (is necesary conection),
        if any error appears in the operation, apply rollback

        Parameters:
            df (dataframe):(required) the dataframe (the same estructure of this table)
            engine : (required) an Engine, which the Session will use for connection
        """
        tbl = self.get_table(engine)
        results=[]
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
                    # Get only primarys keys for clean
                    idpk=[]
                    if str(df.dtypes[pkcols[0]])=="string":
                        idpk = np.unique(["'{}'".format(i) for i in df[pkcols[0]]])
                    else:
                        idpk = np.unique([i for i in df[pkcols[0]]])
                    # drop any coincidence of dataframe cleaned
                    delRow=session.query(tbl).filter(tbl[pk].in_(idpk)).delete()
                    result=session.execute(delRow) 
                    results.append(result)
                else:
                    df_pk_col = df[pkcols] #pick the primary key cols
                    for index, row in df_pk_col.iterrows():
                        delRow = session.query(tbl).filter(**row.to_dict()).delete() #filter and delete for the cols
                        result = session.execute(delRow) 
                        results.append(result)
            except Exception as e:
                session.rollback()
                raise ValueError("Error trying Delete a element of dataframe, apply rollback, Error message [{}]".format(e)) 
            session.commit()
        return results


    def toDb(self, df, engine, method='append', debug=False):
        """
        Insert data of dataframe into database (is necesary conection),
        and apply method for try create database
        Use insert function for add data to db

        Parameters:
            df (dataframe) : (required) the dataframe (the same estructure of this table)
            engine : (required) an Engine, which the Session will use for connection
            method (str): (optional) apply rules before insert table. Aviables:
                - 'append': create the table (if not exist)
                - 'replace': drop and recreate the table (old data is erased)
                - 'clean': clean all data with primary key coincide with the df (require implicit primary key or dataframe with tablename_id column)
            debug (bool) : (optional) if true, show the debug message. Default: False
            
        if you not need apply any mehod, for better opcion, use 'append' method or
        use insert function 

        Returns:
            (array) : results of operations
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Error, df is not dataframe.") 
        tbl = self.get_table(engine)
        results=[]
        with Session(engine) as session:
            session.begin()
            try:
                if method=="append":
                    results.append(self.Base.metadata.tables[self.name].create(engine, checkfirst=True))
                if method=="replace":
                    results.append(self.Base.metadata.tables[self.name].drop(engine, checkfirst=True) )
                    results.append(self.Base.metadata.tables[self.name].create(engine, checkfirst=True))
                if method=="clean":
                   results.append(self.clean(df, engine))

                results.append(self.dataframe_insert(df, engine, debug))
            except Exception as e:
                session.rollback()
                results.append("Error trying insert a element of dataframe, apply rollback, Error message [{}]".format(e)) 
                raise ValueError("Error trying insert a element of dataframe, apply rollback, Error message [{}]".format(e)) 
            else:
                session.commit()
            session.commit()
        return results
        

