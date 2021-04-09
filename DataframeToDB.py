
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from sqlalchemy import Column, Integer, String, Float, Text, BigInteger, Time, Date, DateTime
# import db

import pandas as pd

import datetime as dt
from datetime import datetime
import re 
import numpy as np

from dateutil.parser import parse

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
    df =dfParam
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

def toDB(**kwargs):
    if str(type(kwargs.get('tablename', False)))!="<class 'str'>":
        raise ValueError("tablename: could be <class 'str'>") 
        return False

    if str(type(kwargs.get('df', False)))!="<class 'pandas.core.frame.DataFrame'>":
        raise ValueError("df: could be <class 'pandas.core.frame.DataFrame'>") 
        return False

    engine = None

    if kwargs.get('engine', False):
        engine = kwargs.get('engine', False)
    else:
        engine = create_engine('sqlite:///productos.sqlite')

    Session = sessionmaker(bind=engine)
    session = Session() # create a Session
    Base = declarative_base()

    attr_dict={'__tablename__': kwargs.get('tablename', 'default')}

    if kwargs.get('primary_key', None)==None:
        attr_dict = {'__tablename__': kwargs.get('tablename', 'default'),'id': Column(Integer, primary_key=True, auto_increment=True)}

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
        if str(df.dtypes[col])=="Int64":
            if df[col].sample(qForSample).apply(lambda x: x<=255).all().item():
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Integer, primary_key=True, auto_increment=kwargs.get('primary_key', False))
                else:
                    attr_dict[col] = Column(Integer)
                if kwargs.get('debug', False):
                    print("Nomre: {}, Tipo: {}, ColType: Integer, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))
            else:
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(BigInteger, primary_key=True, auto_increment=kwargs.get('primary_key', False))
                else:
                    attr_dict[col] = Column(BigInteger)
                if kwargs.get('debug', False):
                    print("Nomre: {}, Tipo: {}, ColType: BigInteger, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))
        
        elif str(df.dtypes[col])=="string":
            if df[col].sample(qForSample).apply(lambda x: len(str(x))<=255).all().item():
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(String, primary_key=True)
                else:
                    attr_dict[col] = Column(String)
                if kwargs.get('debug', False):
                    print("Nomre: {}, Tipo: {}, ColType: String, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
            else:
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Text, primary_key=True)
                else:
                    attr_dict[col] = Column(Text)
                if kwargs.get('debug', False):
                    print("Nomre: {}, Tipo: {}, ColType: Text, min: {}, max: {}".format(col, str(df.dtypes[col]), len(df[col].min()), len(df[col].max())))
        elif str(df.dtypes[col])=='datetime64[ns]': #si es date time, puede ser datetime, date or time
            # sample = pd.Timestamp(df[col].sample(1).values[0])
            if df[col].sample(qForSample).apply(lambda x: isDateFromDatetime(x)).all().item():#sample==dt.time(hour=sample.hour, minute=sample.minute, second=sample.microsecond, microsecond=sample.microsecond):#si es time
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Date, primary_key=True)
                else:
                    attr_dict[col] = Column(Date)
                if kwargs.get('debug', False):
                    print("Nomre: {}, Tipo: {}, ColType: Date".format(col, str(df.dtypes[col])))
            elif df[col].sample(qForSample).apply(lambda x: isTimeFromDatetime(x)).all().item():# sample==dt.date(year=sample.year, month=sample.month, day=sample.day):
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(Time, primary_key=True)
                else:
                    attr_dict[col] = Column(Time)
                if kwargs.get('debug', False):
                    print("Nomre: {}, Tipo: {}, ColType: Time".format(col, str(df.dtypes[col])))
            else: #dt.datetime(year=sample.year, month=sample.month, day=sample.day, hour=sample.hour, minute=sample.minute, second=sample.microsecond, microsecond=sample.microsecond)
                if kwargs.get('primary_key', None)==col:
                    attr_dict[col] = Column(DateTime, primary_key=True)
                else:
                    attr_dict[col] = Column(DateTime)
                if kwargs.get('debug', False):
                    print("Nomre: {}, Tipo: {}, ColType: DateTime".format(col, str(df.dtypes[col])))
        else:

            if kwargs.get('debug', False):
                print("Nomre: {}, Tipo: {}, min: {}, max: {}".format(col, str(df.dtypes[col]), df[col].min(), df[col].max()))


    # attr_dict["q"] = Column(Integer)
    

    tabla = type('ClassnameHere', (Base,), attr_dict)

    Base.metadata.create_all(engine)

    SomeRow = tabla(id='2')
    session.add(SomeRow)
    session.commit()

if __name__ == "__main__":
    df = pd.read_csv("./dataset/cv19/covid_19_data.csv") 
    df = refactor(df)
    toDB(tablename="test3", df=df, debug= True)