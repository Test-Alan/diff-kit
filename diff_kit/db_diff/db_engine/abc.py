# @Project: diff-kit
# @Time: 2024/9/2 13:24
# @Author: Alan
# @File: abc

import abc


class DbEngine(metaclass=abc.ABCMeta):
    def __init__(self, host, port, user, password, db):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.pool = None

    @abc.abstractmethod
    def create_pool(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def query(self, columns, table_mame, where_clause=None, start_index=None, batch_size=None, values=None):
        pass

    @abc.abstractmethod
    def query_in(self, columns, table_mame, values):
        pass

    @abc.abstractmethod
    def get_row_count(self, table_name, where_clause=None):
        pass

    @abc.abstractmethod
    def get_table_columns(self, table_name):
        pass
