import sqlite3
from .types import TableRecord
from .errors import *


class DBWorker:
    """
    :param filename: Name of database file
    :type filename: str

    :param if_exists: Use IF EXISTS checks (default - False)
    :type filename: bool
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

    def read(self, tablename, value, conditions=None, raw=False, **kwargs):
        """
        :param tablename: Table name
        :type tablename: str

        :param value: Column name of value to read
        :type value: str

        :param conditions: Conditions to read exactly (default - None)
        :type conditions: dict or list or None

        :param raw: Return raw result (default - False)
        :type raw: bool

        :param kwargs: As conditions
        """

        if type(conditions) == dict:
            clauses = list(conditions.items())
        if len(kwargs) > 0:
            if conditions is not None:
                conditions += list(kwargs.items())
            else:
                conditions = list(kwargs.items())

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

    def write(self, tablename, data=None, **kwargs):
        """
        :param tablename: Table name
        :type tablename: str

        :param data: Data to write, [["column1", ["value1, "value2]], ["column2, ["value1", "value2"]] or
                                    {"column1": ["value1", "value2"], "column2": ["value1", "value2"]}
        :type data: dict or list

        :param kwargs: As data
        """

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

    def update(self, tablename, data, conditions, **kwargs):
        """
        :param tablename: Table name
        :type tablename: str

        :param data: Data to update
        :type data: dict or list

        :param conditions: Conditions to update exactly (default - None)
        :type conditions: dict or list

        :param kwargs: As conditions
        """

        if type(conditions) == dict:
            conditions = list(conditions.items())
        if len(kwargs) > 0:
            if conditions is not None:
                conditions += list(kwargs.items())
            else:
                conditions = list(kwargs.items())

        if type(data) == dict:
            data = list(data.items())

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

    def delete(self, tablename, conditions=None, **kwargs):
        """
        :param tablename: Table name
        :type tablename: str

        :param conditions: Conditions to what delet exactly (default - None)
        :type conditions: dict or list

        :param kwargs: as conditions
        """
        if type(conditions) == dict:
            conditions = list(conditions.items())
        if len(kwargs) > 0:
            if conditions is not None:
                conditions += list(kwargs.items())
            else:
                conditions = list(kwargs.items())
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


