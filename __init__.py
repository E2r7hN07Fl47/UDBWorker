import sqlite3
from .types import TableRecord


class DBWorker:
    def __init__(self, filename):
        self.filename = filename

    def create(self, record: TableRecord):
        pass

    def read(self):
        pass

    def write(self):
        pass

    def delete(self):
        pass

    def check(self):
        pass


