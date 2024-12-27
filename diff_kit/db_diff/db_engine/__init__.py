# @Project: diff-kit
# @Time: 2024/9/2 13:24
# @Author: Alan
# @File: __init__.py
from diff_kit.db_diff.db_engine.abc import DbEngine
from diff_kit.db_diff.db_engine.mysql import AsyncMysqlDbEngine
from diff_kit.db_diff.db_engine.pgsql import AsyncPgEngine

db_mapping = {
    'mysql': AsyncMysqlDbEngine,
    'pgsql': AsyncPgEngine
}

class DbEngineFactory(object):
    def __init__(self, db_type, **kwargs):
        self.db_type = db_type
        self.kwargs = kwargs

    def create_db_engine(self) -> DbEngine:
        db_engine = self.get_db_engine(self.db_type)
        return db_engine(**self.kwargs)
    @staticmethod
    def get_db_engine(db_type):
        if db_type not in db_mapping:
            raise ValueError(f'db_type: {db_type} not supported')
        return db_mapping[db_type]
