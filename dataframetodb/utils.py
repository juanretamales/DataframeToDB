from dateutil.parser import parse
import json
import numpy as np
import pandas as pd

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

def hasDuplicateCols(df, case="insensitive", debug=False):
    """
    Returns duplicate column names (case insensitive)

    Parameters:
        df (Dataframe):The dataframe which is to be revised.
        case (str): should be: "insensitive" by default , "strict"(clean special simbols an lower chars) or "sensitive"

    Returns:
        (bool): True if has any duplicate cols, or false in other case

    """
    cols=[]
    #first get cols for each case
    if case=="strict":
        cols = [cleanSpecialCharacters(c).lower() for c in df.columns]
    elif case=="insensitive":
        cols = [c.lower() for c in df.columns]
    elif case=="sensitive":
        cols = [c.lower() for c in df.columns]
    dups = [x for x in cols if cols.count(x) > 1]

    if dups!=[]:
        if debug:
            print("The duplicate cols: {}".format(','.join(c for c in dups)))
        return False

    return True

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
def tryGet(variable, key, caseFalse=False, caseTruth=None):
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
    return caseFalse

def isTimeFromDatetime(fecha):
    """
    Review if Datetime has time values

    Parameters:
        fecha (Datetime): the String who clean of character of valir_chars variable

    Returns:
        (bool): True if has Time values, or false in other case
    """
    try:
        if fecha.year!=0 and fecha.month!=0 and fecha.day!=0:
            return True
    except:
        return False
    return False

def isDateFromDatetime(fecha):
    """
    Review if Datetime has Date values

    Parameters:
        fecha (Datetime): the String who clean of character of valir_chars variable

    Returns:
        (bool): True if has Date values, or false in other case
    """
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

# # experimental function
# def imageToString(image):
#     """
#     Review if Datetime has Date values

#     Parameters:
#         image (array): array for convert to string

#     Returns:
#         (bool): True if has Date values, or false in other case
#     """
#     return json.dumps(image)

# def stringToImage(string):
#     """
#     Review if Datetime has Date values

#     Parameters:
#         string (str): string to convert to image

#     Returns:
#         (bool): True if has Date values, or false in other case
#     """
#     return np.array(json.loads(string))