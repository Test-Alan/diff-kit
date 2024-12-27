# @Project: diff-kit
# @Time: 2024/12/27 15:28
# @Author: Alan
# @File: test_db_diff

from diff_kit.db_diff.core import DbDiffRunner
from diff_kit.db_diff.core.compare_data import DiffParams

def db_diff():

    DbDiffRunner(
        report_name="test",
        diff_params=DiffParams(
            db_conn_a={
                "db_type": "pgsql",
                "host": "127.0.0.1",
                "port": 5432,
                "user": "root",
                "password": "123456",
            },
            db_conn_b={
                "db_type": "mysql",
                "host": "127.0.0.1",
                "port": 3306,
                "user": "root",
                "password": "123456",
            },
            db_name_a='qa',
            db_name_b='qa',
            table_name_a='schemas.table_name',
            table_name_b='schemas.table_name',
            unique_field=['id'],
        )

    ).diff()

if __name__ == '__main__':
    db_diff()