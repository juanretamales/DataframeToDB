
from dataframetodb.utils import tryGet
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
        if kwargs.get('col_name', False)==False:
            raise ValueError("The name of column of Table cant be empty or invalid")

        if kwargs.get('validType', kwargs.get('type', False))==False:
            raise ValueError("The type cant be empty or invalid")

        self.col_name = kwargs.get('col_name', None)
        self.col_df_name = kwargs.get('col_df_name', kwargs.get('col_name', None))
        self.type = kwargs.get('type', None)
        self.primary = kwargs.get('primary_key', False)
        self.is_nullable = tryGet('nullable', False)

        if (kwargs.get('type', False)=="Integer" or kwargs.get('type', False)=="BigInteger") and kwargs.get('auto_increment', False):
            self.ai = True
        else:
            self.ai = False

        self.fk=None #value 'parent.id' for create ForeignKey('parent.id') #tablename.column parent
        self.rs=None #relationship("Child") #tablename child

    def __str__(self):
        return "{} | {} | {} | {} | {}".format(self.col_name, self.type, "Primary Key" if self.primary else '', "Auto Increment" if self.ai else '', "Nullable" if self.is_nullable else '')

    def get_dict(self):
        return {"col_name":self.col_name, "col_df_name":self.colDfName, "type": self.type, "primary_key": self.primary, "auto_increment": self.ai, "nullable": self.is_nullable}

    def col_data(self):
        if self.primary and self.is_nullable and self.ai:
            return self.col_name, sqlCol(self.validType(self.type), primary_key=self.primary, autoincrement=self.ai, nullable=self.is_nullable)
        elif self.primary and self.ai:
            return self.col_name, sqlCol(self.validType(self.type), primary_key=self.primary, autoincrement=self.ai)
        elif self.primary:
            return self.col_name, sqlCol(self.validType(self.type), primary_key=self.primary)
        elif self.ai:
            return self.col_name, sqlCol(self.validType(self.type), autoincrement=self.ai)
        elif self.is_nullable:
            return self.col_name, sqlCol(self.validType(self.type), nullable=self.is_nullable)
        return self.col_name, sqlCol(self.col_name, self.validType(self.type))

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