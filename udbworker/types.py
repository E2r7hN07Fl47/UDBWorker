from .errors import *


INT = "INTEGER"
TEXT = "TEXT"
BLOB = "BLOB"
REAL = "REAL"
NUM = "NUMERIC"


class TableRecord:
    def __init__(self,
                 name: str,
                 entry_type: str,
                 is_not_null=False,
                 is_unique=False,
                 is_primary=False,
                 is_auto_inc=False):

        self.name = name
        self.type = entry_type
        self.is_not_null = is_not_null
        self.is_unique = is_unique
        self.is_primary = is_primary
        self.is_auto_inc = is_auto_inc

        if not (self.type == INT or self.type == TEXT or self.type == BLOB or self.type == REAL or self.type == NUM):
            raise CreateTypeError("Unknown type")

        if self.is_auto_inc and not self.is_primary:
            raise CreateTypeError("Auto-Increment must be with Primary Key")

        if self.is_auto_inc and self.type != INT:
            raise CreateTypeError("Auto-Increment must be only on INTEGER")

        sql_record = f"{self.name}   {self.type}"

        if self.is_not_null:
            sql_record += " NOT NULL"

        if self.is_unique:
            sql_record += " UNIQUE"

        self.sql_record = sql_record

