import sqlite3
from .types import TableRecord
from .errors import *


class DBWorker:
    def __init__(self, filename, if_exists=False):
        self.filename = filename
        self.if_exists = if_exists

    def _get_conn_cursor(self):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        return conn, cursor

    def _commit_and_close(self, conn, cursor, command):
        cursor.execute(command)
        results = cursor.fetchall()
        conn.commit()
        conn.close()
        return results


    def create(self, tablename, *records):
        conn, cursor = self._get_conn_cursor()

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
        self._commit_and_close(conn, cursor, sql_command)

    def read(self, tablename, value, clauses=None, raw=False, **kwargs):
        conn, cursor = self._get_conn_cursor()
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
        result = self._commit_and_close(conn, cursor, sql_command)
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

    def write(self):
        pass

    def delete(self):
        pass

    def check(self):
        pass

    def remove(self, tablename):
        conn, cursor = self._get_conn_cursor()
        if self.if_exists:
            sql_command = f"DROP TABLE IF EXISTS {tablename};"
        else:
            sql_command = f"DROP TABLE {tablename};"

        self._commit_and_close(conn, cursor, sql_command)


