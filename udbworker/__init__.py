import sqlite3
from .types import *
from .errors import *
from typing import Union, Any


class DBWorker:
    """
    :param filename: Name of database file
    :type filename: str

    :param if_exists: Use IF EXISTS checks (default - False)
    :type if_exists: bool
    """

    def __init__(self, filename: str, if_exists: bool = False) -> None:
        self.filename = filename
        self.if_exists = if_exists

    def _execute_sql(self, command: str) -> list:
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        cursor.execute(command)
        results = cursor.fetchall()
        conn.commit()
        conn.close()
        return results

    def create(self, tablename: str, *records: TableRecord) -> None:
        """
        :param tablename: Table name
        :type tablename: str

        :param records: Columns
        :type records: TableRecord
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

    def read(self, tablename: str, value: Union[str, list, tuple],
             conditions: Union[dict, list, tuple] = (), is_like: Union[dict, list, tuple] = (),
             raw: bool = False, **kwargs) -> Any:
        """
        :param tablename: Table name
        :type tablename: str

        :param value: Column name of value to read
        :type value: Union[str, list, tuple]

        :param conditions: Conditions to read, equals (default - empty tuple)
        :type conditions: Union[dict, list, tuple]

        :param is_like: Conditions to read, pattern ('%' - for any count, '_' for one in query) (default - empty tuple)
        :type is_like: Union[dict, list, tuple]

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

        if is_like is None:
            is_like = []
        like_type = type(is_like)

        if like_type == tuple:
            is_like = list(is_like)
        elif like_type == dict:
            is_like = list(is_like.items())

        if len(is_like) > 0:
            if not (type(is_like[0]) == list or type(is_like[0]) == tuple):
                is_like = [is_like]

        if len(kwargs) > 0:
            conditions += list(kwargs.items())

        if type(value) in (list, tuple):
            value = ", ".join(str(v) for v in value)

        sql_command = f"SELECT {value} FROM {tablename}"
        if conditions is not None:
            sql_command += " WHERE "
            for cond in conditions:
                column, key = cond
                if key is None:
                    sql_command += f" {column} is NULL AND "
                elif "'" in str(key):
                    sql_command += f'{column}="{key}" AND '
                else:
                    sql_command += f"{column}='{key}' AND "
            sql_command = sql_command[:-5]

        if is_like is not None:
            if conditions is None:
                sql_command += " WHERE "
            else:
                sql_command += " AND "
            for cond in is_like:
                column, key = cond
                if "'" in str(key):
                    sql_command += f'{column} LIKE "{key}" AND '
                else:
                    sql_command += f"{column} LIKE '{key}' AND "
            sql_command = sql_command[:-5]

        sql_command += ";"
        result = self._execute_sql(sql_command)
        if raw:
            return result

        if len(result) == 0:
            return None

        if len(result) == 1:
            result = result[0]
            if len(result) == 1 and ',' not in value:
                result = result[0]
        else:
            ret = []
            for res in result:
                if len(res) == 1:
                    ret.append(res[0])
            result = ret
        return result

    def write(self, tablename: str, data: Union[dict, list, tuple], **kwargs) -> None:
        """
        :param tablename: Table name
        :type tablename: str

        :param data: Data to write, [["column1", ["value1", "value2"]], ["column2, ("value1", "value2")] or
                                     {"column1": ["value1", "value2"],  "column2":  ["value1", "value2"]}
        :type data: Union[dict, list, tuple]

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
                if key is None:
                    sql_command += "NULL, "
                elif "'" in str(key):
                    sql_command += f'"{key[i]}", '
                else:
                    sql_command += f"'{key[i]}', "
            sql_command = sql_command[:-2]
            sql_command += "), ("
        sql_command = sql_command[:-3]
        sql_command += ";"
        self._execute_sql(sql_command)

    def update(self, tablename: str, data: Union[dict, list, tuple],
               conditions: Union[dict, list, tuple] = (), **kwargs) -> None:
        """
        :param tablename: Table name
        :type tablename: str

        :param data: Data to update
        :type data: Union[dict, list, tuple]

        :param conditions: Conditions to update exactly (default - empty tuple)
        :type conditions: Union[dict, list, tuple]

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

        sql_command = f"UPDATE {tablename} SET "
        for record in data:
            column, key = record
            if key is None:
                sql_command += f"{column}=NULL, "
            elif "'" in str(key):
                sql_command += f'{column}="{key}", '
            else:
                sql_command += f"{column}='{key}', "
        sql_command = sql_command[:-2]
        if len(conditions) > 0:
            sql_command += " WHERE "
            for cond in conditions:
                column, key = cond
                if key is None:
                    sql_command += f"{column} is NULL AND "
                elif "'" in str(key):
                    sql_command += f'{column}="{key}" AND '
                else:
                    sql_command += f"{column}='{key}' AND "
            sql_command = sql_command[:-5]
        sql_command += ";"
        self._execute_sql(sql_command)

    def delete(self, tablename: str, conditions: Union[dict, list, tuple] = (), **kwargs) -> None:
        """
        :param tablename: Table name
        :type tablename: str

        :param conditions: Conditions to what delete exactly (default - empty tuple)
        :type conditions: Union[dict, list, tuple]

        :param kwargs: as conditions
        """

        cond_type = type(conditions)

        if cond_type == tuple:
            conditions = list(conditions)
        elif cond_type == dict:
            conditions = list(conditions.items())

        if len(kwargs) > 0:
            conditions += list(kwargs.items())

        if len(conditions) > 0:
            if not (type(conditions[0]) == list or type(conditions[0]) == tuple):
                conditions = [conditions]

        sql_command = f"DELETE FROM {tablename}"
        if len(conditions) > 0:
            sql_command += "  WHERE "
            for cond in conditions:
                column, key = cond
                if key is None:
                    sql_command += f"{column} is NULL AND "
                elif "'" in str(key):
                    sql_command += f'{column}="{key}" AND '
                else:
                    sql_command += f"{column}='{key}' AND "
            sql_command = sql_command[:-5]
        sql_command += ";"
        self._execute_sql(sql_command)

    def check(self):
        pass

    def remove_table(self, tablename: str) -> None:
        """
        :param tablename: Table name
        :type tablename: str
        """

        if self.if_exists:
            sql_command = f"DROP TABLE IF EXISTS {tablename};"
        else:
            sql_command = f"DROP TABLE {tablename};"
        self._execute_sql(sql_command)

    def execute_raw(self, sql_command: str) -> list:
        """
        :param sql_command: Raw SQL query, use only if had to.
        :type sql_command: str
        """

        return self._execute_sql(sql_command)
