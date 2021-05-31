
from DataframeToDB import tryGet
from sqlalchemy import BigInteger, Boolean, Date, DateTime, Enum, Float, Integer, Interval, LargeBinary, Numeric, PickleType, SmallInteger, String, Text, Time, Unicode, UnicodeText #, MatchType, SchemaType
from sqlalchemy import Column as sqlCol
import json
import os
import pandas as pd
import datetime as dt
from datetime import datetime
import numpy as np

class Column:
    def __init__(self, **kwargs):
        if kwargs.get('colName', False)==False:
            raise ValueError("The name of column of Table cant be empty or invalid")

        if kwargs.get('validType', kwargs.get('Type', False))==False:
            raise ValueError("The type cant be empty or invalid")

        self.colName = kwargs.get('colName', None)
        self.colDfName = kwargs.get('colDfName', kwargs.get('colName', None))
        self.type = kwargs.get('Type', None)
        self.primary = kwargs.get('PrimaryKey', False)
        self.isnullable = tryGet('nullable', False)

        if (kwargs.get('type', False)=="Integer" or kwargs.get('type', False)=="BigInteger") and kwargs.get('Auto Increment', False):
            self.ai = True
        else:
            self.ai = False

        self.fk=None #value 'parent.id' for create ForeignKey('parent.id') #tablename.column parent
        self.rs=None #relationship("Child") #tablename child

    def __str__(self):
        return "{} | {} | {} | {} | {}".format(self.colName, self.type, "Primary Key" if self.primary else '', "Auto Increment" if self.ai else '', "Nullable" if self.isnullable else '')

    def getDict(self):
        return {"colName":self.colName, "colDfName":self.colDfName, "Type": self.type, "PrimaryKey": self.primary, "AutoIncrement": self.ai, "Nullable": self.isnullable}

    def colData(self):
        if self.primary and self.isnullable and self.ai:
            return self.colName, sqlCol(self.validType(self.type), primary_key=self.primary, autoincrement=self.ai, nullable=self.isnullable)
        elif self.primary and self.ai:
            return self.colName, sqlCol(self.validType(self.type), primary_key=self.primary, autoincrement=self.ai)
        elif self.primary:
            return self.colName, sqlCol(self.validType(self.type), primary_key=self.primary)
        elif self.ai:
            return self.colName, sqlCol(self.validType(self.type), autoincrement=self.ai)
        elif self.isnullable:
            return self.colName, sqlCol(self.validType(self.type), nullable=self.isnullable)
        return self.colName, sqlCol(self.colName, self.validType(self.type))

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