from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, MetaData, Table, create_engine
from sqlalchemy import BigInteger, Boolean, Date, DateTime, Enum, Float, Integer, Interval, LargeBinary, Numeric, PickleType, SmallInteger, String, Text, Time, Unicode, UnicodeText #, MatchType, SchemaType

from sqlalchemy.schema import DropTable, CreateTable
from sqlalchemy.orm import scoped_session, sessionmaker

from contextlib import contextmanager

import pandas as pd

import datetime as dt
from datetime import datetime
import re 
import numpy as np
import json

from dateutil.parser import parse

import os

def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False

#copy from fast-to-sql
def hasDuplicateCols(df, case="insensitive"):
    """
    Returns duplicate column names (case insensitive)

    Parameters:
        df (Dataframe):The dataframe which is to be revised.

    Returns:
        (str): the cleaned String

    """
    cols = [c.lower() for c in df.columns]
    dups = [x for x in cols if cols.count(x) > 1]
    if dups:
        raise errors.DuplicateColumns(f"There are duplicate column names. Repeated names are: {dups}. SQL Server dialect requires unique names (case insensitive).")

def cleanSpecialCharacters(texto):
    """
    Removes special characters from texto

    Parameters:
        texto (str): the String who clean of character of valir_chars variable

    Returns:
        (str): the cleaned String
    """
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    return ''.join(c for c in texto if c in valid_chars)

# def isDateTime(datetime):
#     if sample.year!=0 and sample.month!=0 and sample.day!=0 and sample.hour!=0 and sample.minute!=0 and sample.microsecond!=0 and sample.microsecond!=0:
#         return True
#     return False

def isTimeFromDatetime(fecha):
    try:
        if fecha.year!=0 and fecha.month!=0 and fecha.day!=0:
            return True
    except:
        return False
    return False

def isDateFromDatetime(fecha):
    try:
        if fecha.hour!=0 and fecha.minute!=0 and fecha.microsecond!=0 and fecha.microsecond!=0:
            return True
    except:
        return False
    return False

def refactor(dfParam, estrict=False, debug=False):
    """
    Returns a dataframe with the correct dtype compatible with DataframeToDB.

    Parameters:
        dfParam (Dataframe):The dataframe which is to be refactored.

    Returns:
        df (Dataframe):The dataframe which gets refactored.
    """
    df = dfParam
    df = df.convert_dtypes()
    df = df.fillna(method="pad")

    # #No terminado aun, inferia los tipos de objetos a detalle y no solo con convert_dtypes
    if estrict:
        for col in df.columns:
            # data = df[col]

            # if df.dtypes[col] == 'object': #revisa si la columna es object
            if str(df.dtypes[col])=="string": #revisa si la columna es object
                cant = int(len(df)*0.3) if len(df) < 1000 else 100
                data = df[col].sample(cant) #obtengo el 30% de los datos para verificar los datos

                # Check DATETIME
                if data.apply(lambda x: len(x)==19 ).all().item(): #revisa si tiene la cantidad minima de caracteres para ser datetime
                    print("es DATETIME")

                #check DATE
                elif data.apply(lambda x: is_date(x)).all().item(): #reviso si todos los datos de la query son de tipo Date
                    df[col] = df[col].apply(lambda x: parse(x, fuzzy=False))
                    if df.dtypes[col] != 'datetime64[ns]': #por lo general cambia automaticamente de formato, pero de no hacerlo, lo fuerza
                        df[col] = pd.to_datetime(df[col]).dt.date

                #check Number
                else:
                    df[col].apply(pd.to_numeric, errors='ignore') #cambia la columna a tipo numero, int o float y los nan o errores los cambia a 0

            # elif df.dtypes[col] == 'object':
            #     print("es object")
                
            else:
                if debug:
                    print("Else {} no es soportado aun".format(df.dtypes[col]))

    return df

def tryGet(variable, key, case=False, caseTruth=None):
    """
    Returns a value depend of if variable has a key, in other case return case

    Parameters:
        variable : Variable who has a key, prefer list or dict
        key : is the key for get data in the variable
        case (optional): value who return when variable has not key


    Returns:
        case or value : the value get of variable if exist or case
    """
    try:
        var = variable[key]
        if caseTruth!=None:
            return caseTruth
        return var
    except:
        pass
    return case


class Database:
    def __init__(self, name, tablas=[[]]):
        self.nombre = name
        # matriz, revisa desde lo mas alto la dependencia para agregar
        self.tablas=tablas
        # self.folder=None

    def addTable(self, tabla):
        nivel = -1
        for lvl in self.tablas:
            for tbl in lvl:
                if tabla.nombre == tbl:
                    print("fail")


    def loadFromFile(self):
        if os.path.exists(os.path.join('.dataframeToDb', self.nombre)):
            print("sucess")

    def saveToFile(self):
        if not os.path.exists('.dataframeToDb'):
            try:
                os.makedirs(".dataframeToDb")
            except OSError as e:
                if e.errno != e.errno.EXIST:
                    raise

        if not os.path.exists(os.path.join('.dataframeToDb', self.nombre)):
            try:
                os.makedirs(os.path.join('.dataframeToDb', self.nombre))
            except OSError as e:
                if e.errno != e.errno.EXIST:
                    raise

        

class Tabla:
    def __init__(self, **kwargs):
        if str(type(kwargs.get('Df', False)))!="<class 'pandas.core.frame.DataFrame'>":
            raise ValueError("Df: could be <class 'pandas.core.frame.DataFrame'>") 
        #if define 1, not both with XOR operator
        if tryGet(kwargs, 'Df', False, True)==False ^ kwargs.get('Columns', False)==False:
            raise ValueError("Only can define Dataframe or Columns, not both") 

        self.columns=[]
        if tryGet(kwargs, 'Df', False, True):
            self.columns = [] #convert Df -> Columns
        if tryGet(kwargs, 'Columns', False, True):
            cols = [isinstance(col, Columna) for col in tryGet(kwargs, 'Columns', [])]
            if np.all(cols):
                self.columns = tryGet(kwargs, 'Columns')
            else:
                raise ValueError("A column it not a Column type class") 

        self.nombre = kwargs.get('Nombre', "NoName")
        self.file=kwargs.get('File', None)
        if self.file==None:
            self.file=os.path.join('.dataframeToDb', str(self.nombre) + ".ToDB")

    def dataframeToColumnas(self):
        raise ValueError("Not implemented yet") 

    def getSQLcolumns(self):
        column =[Column(col.colData())for col in self.columas]
        return column

    def getParents(self):
        column =[col for col in self.columas if col.fk!=None]
        return column

    def getTable(self):
        return Table(self.nombre, MetaData(), *self.getSQLcolumns())

    def getDict(self):
        """
        Returns a dict in values of this Table class with the columnd dict values

        Returns:
            dict : the values of this class in dict format
        """
        return {
            "Name": self.nombre, 
            "File": self.file,
            "Type": "Table",
            "Columns": [col.data() for col in self.columns]
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
            #Verifi if path exist, if false, create the path
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
            print("DataframeToDB: Error, the data is not a table")
            return False
        if tryGet(json, "Type")!="Name":
            print("DataframeToDB: Error, the data not have name")
            return False
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
                Columna(
                    {
                        "col_name": tryGet(col, "col_name"),
                        "col_dfName": tryGet(col, "col_dfName", tryGet(col, "col_name")),
                        "Type": tryGet(col, "Type"),
                        "primary": tryGet(col, "Primary Key", False),
                        "Auto Increment": tryGet(col, "Auto Increment", False)
                    }
                )
            )


class Columna:

    def __init__(self, **kwargs):

        if kwargs.get('colName', False)==False:
            raise ValueError("The name of column of Table cant be empty or invalid")

        if kwargs.get('validType', kwargs.get('Type', False))==False:
            raise ValueError("The type cant be empty or invalid")

        self.colName = kwargs.get('colName', None)
        #if none dfName is definited, use col_name
        self.colDfName = kwargs.get('colDfName', kwargs.get('colName', None))
        self.type = kwargs.get('Type', None)
        self.primary = kwargs.get('Primary Key', False)

        if (kwargs.get('type', False)=="Integer" or kwargs.get('type', False)=="BigInteger") and kwargs.get('Auto Increment', False):
            self.ai = True
        else:
            self.ai = False

        self.fk=None #value 'parent.id' for create ForeignKey('parent.id') #tablename.column parent
        self.rs=None #relationship("Child") #tablename child

    def __str__(self):
        return "{} | {} | {} | {}".format(self.colName, self.type, "Primary Key" if self.primary else '', "Auto Increment" if self.ai else '')

    def getDict(self):
        return {"colName":self.colName, "colDfName":self.colDfName, "Type": self.type, "Primary Key": self.primary, "Auto Increment": self.ai}

    def colData(self):
        return (self.colName, self.type, "Primary Key" if self.primary else '', "Auto Increment" if self.ai else '')

    def validType(self, text):
        types={
            "Integer": Integer, 
            "BigInteger": BigInteger,
            "String": String,
            "Text": Text,
            "Date": Date,
            "Time": Time,
            "Float": Float,
            "DateTime": DateTime,
            "Boolean": Boolean,
            "Enum": Enum,
            "Interval": Interval,
            "LargeBinary": LargeBinary,
            "Numeric": Numeric,
            "PickleType": PickleType,
            "SmallInteger": SmallInteger,
            "Unicode": Unicode,
            "UnicodeText": UnicodeText,
        }
        if text in types:
            return types[text]
        return False


def generate(**kwargs):
    """
    Returns a sqlalchemy table with dataframe

    Parameters:
        variable : Variable who has a key, prefer list or dict
        key : is the key for get data in the variable
        case (optional): value who return when variable has not key


    Returns:
        case or value : the value get of variable if exist or case
    """
    if str(type(kwargs.get('tablename', False)))!="<class 'str'>":
        raise ValueError("tablename: could be <class 'str'>") 
        return False

    if str(type(kwargs.get('df', False)))!="<class 'pandas.core.frame.DataFrame'>":
        raise ValueError("df: could be <class 'pandas.core.frame.DataFrame'>") 
        return False

    tablename = kwargs.get('tablename', 'default')

    engine = None

    if kwargs.get('engine', False):
        engine = kwargs.get('engine', False)
    else:
        engine = create_engine('sqlite:///{}.sqlite'.format(tablename))

    Session = sessionmaker(bind=engine)
    session = Session() # create a Session
    Base = declarative_base()

    attr_dict={'__tablename__': tablename}

    if kwargs.get('primary_key', None)==None:
        attr_dict = {'__tablename__': kwargs.get('tablename', 'default'),'idx': Column(Integer, primary_key=True, auto_increment=True)}

    qForSample = -1 #despues aceptar parametro porcentaje, ejemplo 30% de los datos
    if kwargs.get('qForSample', False):
        qForSample = kwargs.get('qForSample', len(df))
        if qForSample<0:
            raise ValueError("qForSample: could be positive") 
            return False
        if qForSample>len(df):
            print("Warning, qForSample is more than dataframe length, use length instead")
            qForSample=len(df)
    else:
        qForSample = len(df) if len(df)<=100 else int(len(df) * 0.3)

    colList = df.columns.to_list()

    for col in colList:
        custom = tryGet(tryGet(kwargs, 'custom'), col)
        if custom!=False:
            if custom=="Integer":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Integer, primary_key=True, auto_increment=kwargs.get('primary_key', False))
                else:
                    attr_dict[col] = Column(Integer)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Integer, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))
            elif custom=="BigInteger":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(BigInteger, primary_key=True, auto_increment=kwargs.get('primary_key', False))
                else:
                    attr_dict[col] = Column(BigInteger)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: BigInteger, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))
            elif custom=="String":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(String, primary_key=True)
                else:
                    attr_dict[col] = Column(String)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: String, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
            elif custom=="Text":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Text, primary_key=True)
                else:
                    attr_dict[col] = Column(Text)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Text, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
            elif custom=="Date":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Date, primary_key=True)
                else:
                    attr_dict[col] = Column(Date)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Date".format(col, str(df.dtypes[col])))
            elif custom=="Time":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Time, primary_key=True)
                else:
                    attr_dict[col] = Column(Time)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Time".format(col, str(df.dtypes[col])))
            elif custom=="Float":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Float, primary_key=True)
                else:
                    attr_dict[col] = Column(Float)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Float".format(col, str(df.dtypes[col])))
            elif custom=="DateTime":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(DateTime, primary_key=True)
                else:
                    attr_dict[col] = Column(DateTime)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: DateTime".format(col, str(df.dtypes[col])))
            elif custom=="Boolean":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Boolean, primary_key=True)
                else:
                    attr_dict[col] = Column(Boolean)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Boolean".format(col, str(df.dtypes[col])))
            elif custom=="Enum":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Enum, primary_key=True)
                else:
                    attr_dict[col] = Column(Enum)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Enum".format(col, str(df.dtypes[col])))
            elif custom=="Interval":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Interval, primary_key=True)
                else:
                    attr_dict[col] = Column(Interval)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Interval".format(col, str(df.dtypes[col])))
            elif custom=="LargeBinary":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(LargeBinary, primary_key=True)
                else:
                    attr_dict[col] = Column(LargeBinary)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: LargeBinary".format(col, str(df.dtypes[col])))
            # error import
            # elif custom=="MatchType":
            #     if kwargs.get('primary_key', None)==col:
            #         attr_dict[col] = Column(MatchType, primary_key=True)
            #     else:
            #         attr_dict[col] = Column(MatchType)
            #     if kwargs.get('debug', False):
            #         print("Nombre: {}, Tipo: {}, ColType: MatchType".format(col, str(df.dtypes[col])))
            elif custom=="Numeric":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Numeric, primary_key=True)
                else:
                    attr_dict[col] = Column(Numeric)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Numeric".format(col, str(df.dtypes[col])))
            elif custom=="PickleType":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(PickleType, primary_key=True)
                else:
                    attr_dict[col] = Column(PickleType)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: PickleType".format(col, str(df.dtypes[col])))
            # error import
            # elif custom=="SchemaType":
            #     if kwargs.get('primary_key', None)==col:
            #         attr_dict[col] = Column(SchemaType, primary_key=True)
            #     else:
            #         attr_dict[col] = Column(SchemaType)
            #     if kwargs.get('debug', False):
            #         print("Nombre: {}, Tipo: {}, ColType: SchemaType".format(col, str(df.dtypes[col])))
            elif custom=="SmallInteger":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(SmallInteger, primary_key=True)
                else:
                    attr_dict[col] = Column(SmallInteger)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: SmallInteger".format(col, str(df.dtypes[col])))
            elif custom=="Unicode":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Unicode, primary_key=True)
                else:
                    attr_dict[col] = Column(Unicode)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Unicode".format(col, str(df.dtypes[col])))
            elif custom=="UnicodeText":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(UnicodeText, primary_key=True)
                else:
                    attr_dict[col] = Column(UnicodeText)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: UnicodeText".format(col, str(df.dtypes[col])))
            else:
                if kwargs.get('debug', False):
                    print("Error: no soportado Nombre: {}, Tipo: {}".format(col, str(df.dtypes[col])))
                    return False

        elif str(df.dtypes[col])=="Int64":
            if df[col].sample(qForSample).apply(lambda x: x<=-2147483648 and x>=2147483647 ).all().item(): #corregir
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Integer, primary_key=True, auto_increment=kwargs.get('primary_key', False))
                else:
                    attr_dict[col] = Column(Integer)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Integer, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))
            else:
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(BigInteger, primary_key=True, auto_increment=kwargs.get('primary_key', False))
                else:
                    attr_dict[col] = Column(BigInteger)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: BigInteger, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))

        elif str(df.dtypes[col])=="float64":
            if kwargs.get('primary_key', None)==col:
                attr_dict[col] = Column(Float, primary_key=True, auto_increment=kwargs.get('primary_key', False))
            else:
                attr_dict[col] = Column(Float)
            if kwargs.get('debug', False):
                print("Nombre: {}, Tipo: {}, ColType: Float, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))

        elif str(df.dtypes[col])=="boolean":
            if kwargs.get('primary_key', None)==col:
                attr_dict[col] = Column(Boolean, primary_key=True, auto_increment=kwargs.get('primary_key', False))
            else:
                attr_dict[col] = Column(Boolean)
            if kwargs.get('debug', False):
                print("Nombre: {}, Tipo: {}, ColType: Float, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))
        
        elif str(df.dtypes[col])=="string":
            if df[col].sample(qForSample).apply(lambda x: len(str(x))<=255).all().item():
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(String, primary_key=True)
                else:
                    attr_dict[col] = Column(String)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: String, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
            else:
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Text, primary_key=True)
                else:
                    attr_dict[col] = Column(Text)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Text, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
        elif str(df.dtypes[col])=='datetime64[ns]': #si es date time, puede ser datetime, date or time
            # sample = pd.Timestamp(df[col].sample(1).values[0])
            if df[col].sample(qForSample).apply(lambda x: isDateFromDatetime(x)).all().item():#sample==dt.time(hour=sample.hour, minute=sample.minute, second=sample.microsecond, microsecond=sample.microsecond):#si es time
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Date, primary_key=True)
                else:
                    attr_dict[col] = Column(Date)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Date".format(col, str(df.dtypes[col])))
            elif df[col].sample(qForSample).apply(lambda x: isTimeFromDatetime(x)).all().item():# sample==dt.date(year=sample.year, month=sample.month, day=sample.day):
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Time, primary_key=True)
                else:
                    attr_dict[col] = Column(Time)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Time".format(col, str(df.dtypes[col])))
            else: #dt.datetime(year=sample.year, month=sample.month, day=sample.day, hour=sample.hour, minute=sample.minute, second=sample.microsecond, microsecond=sample.microsecond)
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(DateTime, primary_key=True)
                else:
                    attr_dict[col] = Column(DateTime)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: DateTime".format(col, str(df.dtypes[col])))
        else:

            if kwargs.get('debug', False):
                print("No soportado, Nombre: {}, Tipo: {}, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))


    tabla = type('ClassnameHere', (Base,), attr_dict)

    Base.metadata.create_all(engine)

    if kwargs.get('saveToPickle', False):
        print("No soportado, Nombre: {}, Tipo: {}, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))

    return tabla
    

def toDb(**kwargs):
    """
    Sabe a SqlAlchemy Table in db

    Parameters:
        table : sqlAlchemy Table
        engine : is the key for get data in the variable
        df (Dataframe):The dataframe which is to be saved.

    Returns:
        boolean : return true or false if save to db
    """
    if str(type(kwargs.get('tablename', False)))!="<class 'str'>":
        raise ValueError("tablename: could be <class 'str'>") 
        return False

    if str(type(kwargs.get('df', False)))!="<class 'pandas.core.frame.DataFrame'>":
        raise ValueError("df: could be <class 'pandas.core.frame.DataFrame'>") 
        return False

        

    tablename = kwargs.get('tablename', 'default')

    engine = None

    if kwargs.get('engine', False):
        engine = kwargs.get('engine', False)
    else:
        engine = create_engine('sqlite:///{}.sqlite'.format(kwargs.get('database_name', kwargs.get('tablename', 'default'))))

    Session = sessionmaker(bind=engine)
    session = Session() # create a Session
    Base = declarative_base()

    attr_dict={'__tablename__': tablename}

    if kwargs.get('primary_key', None)==None:
        attr_dict = {'__tablename__': kwargs.get('tablename', 'default'),'idx': Column(Integer, primary_key=True, auto_increment=True)}

    qForSample = -1 #despues aceptar parametro porcentaje, ejemplo 30% de los datos
    if kwargs.get('qForSample', False):
        qForSample = kwargs.get('qForSample', len(df))
        if qForSample<0:
            raise ValueError("qForSample: could be positive") 
            return False
        if qForSample>len(df):
            print("Warning, qForSample is more than dataframe length, use length instead")
            qForSample=len(df)
    else:
        qForSample = len(df) if len(df)<=100 else int(len(df) * 0.3)

    colList = df.columns.to_list()

    for col in colList:
        custom = tryGet(tryGet(kwargs, 'custom'), col)
        if custom!=False:
            if custom=="Integer":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Integer, primary_key=True, auto_increment=kwargs.get('primary_key', False))
                else:
                    attr_dict[col] = Column(Integer)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Integer, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))
            elif custom=="BigInteger":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(BigInteger, primary_key=True, auto_increment=kwargs.get('primary_key', False))
                else:
                    attr_dict[col] = Column(BigInteger)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: BigInteger, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))
            elif custom=="String":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(String, primary_key=True)
                else:
                    attr_dict[col] = Column(String)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: String, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
            elif custom=="Text":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Text, primary_key=True)
                else:
                    attr_dict[col] = Column(Text)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Text, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
            elif custom=="Date":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Date, primary_key=True)
                else:
                    attr_dict[col] = Column(Date)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Date".format(col, str(df.dtypes[col])))
            elif custom=="Time":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Time, primary_key=True)
                else:
                    attr_dict[col] = Column(Time)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Time".format(col, str(df.dtypes[col])))
            elif custom=="Float":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Float, primary_key=True)
                else:
                    attr_dict[col] = Column(Float)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Float".format(col, str(df.dtypes[col])))
            elif custom=="DateTime":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(DateTime, primary_key=True)
                else:
                    attr_dict[col] = Column(DateTime)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: DateTime".format(col, str(df.dtypes[col])))
            elif custom=="Boolean":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Boolean, primary_key=True)
                else:
                    attr_dict[col] = Column(Boolean)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Boolean".format(col, str(df.dtypes[col])))
            elif custom=="Enum":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Enum, primary_key=True)
                else:
                    attr_dict[col] = Column(Enum)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Enum".format(col, str(df.dtypes[col])))
            elif custom=="Interval":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Interval, primary_key=True)
                else:
                    attr_dict[col] = Column(Interval)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Interval".format(col, str(df.dtypes[col])))
            elif custom=="LargeBinary":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(LargeBinary, primary_key=True)
                else:
                    attr_dict[col] = Column(LargeBinary)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: LargeBinary".format(col, str(df.dtypes[col])))
            # error import
            # elif custom=="MatchType":
            #     if kwargs.get('primary_key', None)==col:
            #         attr_dict[col] = Column(MatchType, primary_key=True)
            #     else:
            #         attr_dict[col] = Column(MatchType)
            #     if kwargs.get('debug', False):
            #         print("Nombre: {}, Tipo: {}, ColType: MatchType".format(col, str(df.dtypes[col])))
            elif custom=="Numeric":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Numeric, primary_key=True)
                else:
                    attr_dict[col] = Column(Numeric)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Numeric".format(col, str(df.dtypes[col])))
            elif custom=="PickleType":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(PickleType, primary_key=True)
                else:
                    attr_dict[col] = Column(PickleType)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: PickleType".format(col, str(df.dtypes[col])))
            # error import
            # elif custom=="SchemaType":
            #     if kwargs.get('primary_key', None)==col:
            #         attr_dict[col] = Column(SchemaType, primary_key=True)
            #     else:
            #         attr_dict[col] = Column(SchemaType)
            #     if kwargs.get('debug', False):
            #         print("Nombre: {}, Tipo: {}, ColType: SchemaType".format(col, str(df.dtypes[col])))
            elif custom=="SmallInteger":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(SmallInteger, primary_key=True)
                else:
                    attr_dict[col] = Column(SmallInteger)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: SmallInteger".format(col, str(df.dtypes[col])))
            elif custom=="Unicode":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Unicode, primary_key=True)
                else:
                    attr_dict[col] = Column(Unicode)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Unicode".format(col, str(df.dtypes[col])))
            elif custom=="UnicodeText":
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(UnicodeText, primary_key=True)
                else:
                    attr_dict[col] = Column(UnicodeText)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: UnicodeText".format(col, str(df.dtypes[col])))
            else:
                if kwargs.get('debug', False):
                    print("Error: no soportado Nombre: {}, Tipo: {}".format(col, str(df.dtypes[col])))
                    return False

        elif str(df.dtypes[col])=="Int64":
            if df[col].sample(qForSample).apply(lambda x: x<=-2147483648 and x>=2147483647 ).all().item(): #corregir
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Integer, primary_key=True, auto_increment=kwargs.get('primary_key', False))
                else:
                    attr_dict[col] = Column(Integer)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Integer, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))
            else:
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(BigInteger, primary_key=True, auto_increment=kwargs.get('primary_key', False))
                else:
                    attr_dict[col] = Column(BigInteger)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: BigInteger, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))

        elif str(df.dtypes[col])=="Float64":
            if kwargs.get('primary_key', None)==col:
                attr_dict[col] = Column(Float, primary_key=True, auto_increment=kwargs.get('primary_key', False))
            else:
                attr_dict[col] = Column(Float)
            if kwargs.get('debug', False):
                print("Nombre: {}, Tipo: {}, ColType: Float, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))

        elif str(df.dtypes[col])=="boolean":
            if kwargs.get('primary_key', None)==col:
                attr_dict[col] = Column(Boolean, primary_key=True, auto_increment=kwargs.get('primary_key', False))
            else:
                attr_dict[col] = Column(Boolean)
            if kwargs.get('debug', False):
                print("Nombre: {}, Tipo: {}, ColType: Float, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))

        
        
        elif str(df.dtypes[col])=="string":
            if df[col].sample(qForSample).apply(lambda x: len(str(x))<=255).all().item():
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(String, primary_key=True)
                else:
                    attr_dict[col] = Column(String)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: String, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
            else:
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Text, primary_key=True)
                else:
                    attr_dict[col] = Column(Text)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Text, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
        elif str(df.dtypes[col])=='datetime64[ns]': #si es date time, puede ser datetime, date or time
            # sample = pd.Timestamp(df[col].sample(1).values[0])
            if df[col].sample(qForSample).apply(lambda x: isDateFromDatetime(x)).all().item():#sample==dt.time(hour=sample.hour, minute=sample.minute, second=sample.microsecond, microsecond=sample.microsecond):#si es time
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Date, primary_key=True)
                else:
                    attr_dict[col] = Column(Date)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Date".format(col, str(df.dtypes[col])))
            elif df[col].sample(qForSample).apply(lambda x: isTimeFromDatetime(x)).all().item():# sample==dt.date(year=sample.year, month=sample.month, day=sample.day):
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Time, primary_key=True)
                else:
                    attr_dict[col] = Column(Time)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: Time".format(col, str(df.dtypes[col])))
            else: #dt.datetime(year=sample.year, month=sample.month, day=sample.day, hour=sample.hour, minute=sample.minute, second=sample.microsecond, microsecond=sample.microsecond)
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(DateTime, primary_key=True)
                else:
                    attr_dict[col] = Column(DateTime)
                if kwargs.get('debug', False):
                    print("Nombre: {}, Tipo: {}, ColType: DateTime".format(col, str(df.dtypes[col])))
        else:

            if kwargs.get('debug', False):
                print("No soportado, Nombre: {}, Tipo: {}, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))


    tabla = type('ClassnameHere', (Base,), attr_dict)

    Base.metadata.create_all(engine)

    if kwargs.get('debug', False):
        print("starting to save the data in the selected database, you can pray that it does not fail in the meantime")
    # create rows for insert
    for index, row in df.iterrows():
        newRow = tabla(**row.to_dict())
        session.add(newRow)
    session.commit()
    if kwargs.get('debug', False):
        print("it's finished, your pleas were heard")

if __name__ == "__main__":
    # df = pd.read_csv("./dataset/cv19/covid_19_data.csv") 
    df = pd.read_csv("./dataset/stroke prediction dataset/healthcare-dataset-stroke-data.csv") 
    df["bool"] = df["gender"].apply(lambda x: x=="Male")
    df = refactor(df)
    col = Columna(colName="a", Type="Integer")
    sufix = str(datetime.now().date()).replace('-','')  + str(datetime.now().time()).replace(':', '').replace('.','')
    # db = Database("database_Test")
    table = Tabla(Name="database_Test", Df=df)
    table.saveToFile()
    # toDb(tablename="test-{}".format(sufix), df=df, debug= True, )#custom={'ObservationDate':'Text'})
    # save(tablename="test-{}".format(sufix), df=df, debug= True, )#custom={'ObservationDate':'Text'})