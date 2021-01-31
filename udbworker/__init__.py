import sqlite3
from .types import *
from .errors import *


class DBWorker:
    """
    :param filename: Name of database file
    :type filename: str

    :param if_exists: Use IF EXISTS checks (default - False)
    :type if_exists: bool
    """

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
        """
        :param tablename: Table name
        :type tablename: str

        :param records: Columns
        :type records: types.TableRecord
        """

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

    def read(self, tablename, value, conditions=(), raw=False, **kwargs):
        """
        :param tablename: Table name
        :type tablename: str

        :param value: Column name of value to read
        :type value: str

        :param conditions: Conditions to read exactly (default - empty tuple)
        :type conditions: dict or list or tuple


        :param raw: Return raw result (default - False)
        :type raw: bool

        :param kwargs: As conditions
        """

        if conditions is None:
            conditions = []
        cond_type = type(conditions)

        if cond_type == tuple:
            conditions = list(conditions)
        elif cond_type == dict:
            conditions = list(conditions.items())

        if len(conditions) > 0:
            if not (type(conditions[0]) == list or type(conditions[0]) == tuple):
                conditions = [conditions]

        if len(kwargs) > 0:
            conditions += list(kwargs.items())

        sql_command = f"SELECT {value} FROM {tablename}"
        if conditions is not None:
            sql_command += " WHERE"
            for cond in conditions:
                column, key = cond
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

    def write(self, tablename, data=(), **kwargs):
        """
        :param tablename: Table name
        :type tablename: str

        :param data: Data to write, [["column1", ["value1", "value2"]], ["column2, ("value1", "value2")] or
                                     {"column1": ["value1", "value2"],  "column2": "value1"}
                                     (default - empty tuple)
        :type data: dict or list or tuple

        :param kwargs: As data
        """

        data_type = type(data)
        if data_type == dict:
            data = list(data.items())
        elif data_type == tuple:
            data = list(data)

        if len(kwargs) > 0:
            data += list(kwargs.items())

        sql_command = f"INSERT INTO {tablename} ("

        keys = []
        for record in data:
            column = record[0]
            key = record[1]
            key_type = type(key)
            if key_type == tuple:
                key = list(key)
            elif not key_type == list:
                key = [key]
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

    def update(self, tablename, data, conditions=[], **kwargs):
        """
        :param tablename: Table name
        :type tablename: str

        :param data: Data to update
        :type data: dict or list or tuple

        :param conditions: Conditions to update exactly (default - emply list)
        :type conditions: dict or list or tuple

        :param kwargs: As conditions
        """

        cond_type = type(conditions)

        if cond_type == tuple:
            conditions = list(conditions)
        elif cond_type == dict:
            conditions = list(conditions.items())

        if len(conditions) > 0:
            if not (type(conditions[0]) == list or type(conditions[0]) == tuple):
                conditions = [conditions]

        if len(kwargs) > 0:
            conditions += list(kwargs.items())

        data_type = type(data)

        if data_type == tuple:
            data = list(data)
        elif data_type == dict:
            data = list(data.items())

        if len(data) > 0:
            if not (type(data[0]) == list or type(data[0]) == tuple):
                data = [data]

        sql_command = f"UPDATE {tablename} SET"
        for record in data:
            column, key = record
            sql_command += f" {column}='{key}',"
        sql_command = sql_command[:-1]
        sql_command += " WHERE"
        for cond in conditions:
            column, key = cond
            sql_command += f" {column}='{key}',"
        sql_command = sql_command[:-1]
        sql_command += ";"
        self._execute_sql(sql_command)

    def delete(self, tablename, conditions=[], **kwargs):
        """
        :param tablename: Table name
        :type tablename: str

        :param conditions: Conditions to what delet exactly (default - empty list)
        :type conditions: dict or list or tuple

        :param kwargs: as conditions
        """

        cond_type = type(conditions)

        if cond_type == tuple:
            conditions = list(conditions)
        elif cond_type == dict:
            conditions = list(conditions.items())

        if len(conditions) > 0:
            if not (type(conditions[0]) == list or type(conditions[0]) == tuple):
                conditions = [conditions]

        sql_command = f"DELETE FROM {tablename} WHERE "
        for cond in conditions:
            column, key = cond
            sql_command += f"{column}='{key}' AND "
        sql_command = sql_command[:-5]
        sql_command += ";"
        self._execute_sql(sql_command)

    def check(self):
        pass

    def remove_table(self, tablename):
        """
        :param tablename: Table name
        :type tablename: str
        """

        if self.if_exists:
            sql_command = f"DROP TABLE IF EXISTS {tablename};"
        else:
            sql_command = f"DROP TABLE {tablename};"
        self._execute_sql(sql_command)


