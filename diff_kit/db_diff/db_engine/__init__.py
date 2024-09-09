# @Project: diff-kit
# @Time: 2024/9/2 13:24
# @Author: Alan
# @File: __init__.py


from diff_kit.db_diff.db_engine.mysql import AsyncMysqlDbEngine
from diff_kit.db_diff.db_engine.pgsql import AsyncPgEngine

db_mapping = {
    'mysql': AsyncMysqlDbEngine,
    'pgsql': AsyncPgEngine
}


def get_db_engine(db_type):
    if db_type not in db_mapping:
        raise ValueError(f'db_type: {db_type} not supported')
    return db_mapping[db_type]
