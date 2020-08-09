import sqlite3
from .types import TableRecord
from .errors import *


class DBWorker:
    def __init__(self, filename, if_exists=False):
        self.filename = filename
        self.if_exists = if_exists

    def _execute_sql(self, command):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        cursor.execute(command)
        results = cursor.fetchall()
        conn.commit()
        conn.close()
        return results

    def create(self, tablename, *records):
        pks = []
        if self.if_exists:
            sql_command = f"CREATE TABLE IF NOT EXISTS {tablename} (\n"
        else:
            sql_command = f"CREATE TABLE {tablename} (\n"
        for record in records:
            sql_command += record.sql_record + ',\n'
            if record.is_primary:
                pks.append(record)
        if len(pks) == 0:
            sql_command = sql_command[:-2] + '\n'

        is_ai = False
        for record in pks:
            if record.is_primary and is_ai:
                raise CreateTableError("There must be only one primary key with auto-increment")
            if record.is_auto_inc:
                is_ai = True

        if is_ai:
            sql_command += f'PRIMARY KEY("{pks[0].name}" AUTOINCREMENT)\n'
        elif len(pks) > 0:
            all_names = []
            for record in pks:
                all_names.append(record.name)
            sql_command += f'PRIMARY KEY("{",".join(all_names)})"\n'
        sql_command += ");"
        self._execute_sql(sql_command)

    def read(self, tablename, value, clauses=None, raw=False, **kwargs):
        if type(clauses) == dict:
            clauses = list(clauses.items())
        if len(kwargs) > 0:
            if clauses is not None:
                clauses += list(kwargs.items())
            else:
                clauses = list(kwargs.items())

        sql_command = f"SELECT {value} FROM {tablename}"
        if clauses is not None:
            sql_command += " WHERE"
            for cl in clauses:
                column, key = cl
                sql_command += f" {column}='{key}' AND"
            sql_command = sql_command[:-4]
        sql_command += ";"
        result = self._execute_sql(sql_command)
        if raw:
            return result

        if len(result) == 0:
            return None

        if len(result) == 1:
            result = result[0]
            if len(result) == 1:
                result = result[0]
        else:
            ret = []
            for res in result:
                if len(res) == 1:
                    ret.append(res[0])
            result = ret
        return result

    def write(self, tablename, data=None, **kwargs):
        if type(data) == dict:
            data = list(data.items())
        if len(kwargs) > 0:
            if data is not None:
                data += list(kwargs.items())
            else:
                data = list(kwargs.items())
        sql_command = f"INSERT INTO {tablename} ("

        keys = []
        for record in data:
            column = record[0]
            key = list(record[1])
            keys.append(key)
            sql_command += f"{column}, "
        sql_command = sql_command[:-2]
        sql_command += ") VALUES ("
        for i in range(len(keys[0])):
            for key in keys:
                sql_command += f"'{key[i]}', "
            sql_command = sql_command[:-2]
            sql_command += "), ("
        sql_command = sql_command[:-3]
        sql_command += ";"
        self._execute_sql(sql_command)

    def update(self, tablename, data, clauses, **kwargs):
        if type(clauses) == dict:
            clauses = list(clauses.items())
        if len(kwargs) > 0:
            if clauses is not None:
                clauses += list(kwargs.items())
            else:
                clauses = list(kwargs.items())

        if type(data) == dict:
            data = list(data.items())

        sql_command = f"UPDATE {tablename} SET"
        for record in data:
            column, key = record
            sql_command += f" {column}='{key}',"
        sql_command = sql_command[:-1]
        sql_command += " WHERE"
        for cl in clauses:
            column, key = cl
            sql_command += f" {column}='{key}',"
        sql_command = sql_command[:-1]
        sql_command += ";"
        self._execute_sql(sql_command)

    def delete(self):
        pass

    def check(self):
        pass

    def remove(self, tablename):
        if self.if_exists:
            sql_command = f"DROP TABLE IF EXISTS {tablename};"
        else:
            sql_command = f"DROP TABLE {tablename};"
        self._execute_sql(sql_command)


