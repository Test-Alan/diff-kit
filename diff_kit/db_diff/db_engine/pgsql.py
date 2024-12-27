# @Project: diff-kit
# @Time: 2024/9/2 14:38
# @Author: Alan
# @File: pgsql

import asyncpg
from diff_kit.db_diff.db_engine.abc import DbEngine
from diff_kit.db_diff.db_engine.exception import handle_db_exception


class AsyncPgEngine(DbEngine):
    def __init__(self, host, port, user, password, db):
        super().__init__(host, port, user, password, db)

    async def create_pool(self, max_size=10):
        self.pool = await asyncpg.create_pool(
            user=self.user,
            password=self.password,
            database=self.db,
            host=self.host,
            port=self.port,
            max_size=max_size
        )

    async def close(self):
        await self.pool.close()

    def gen_query_sql(self, columns, table_mame, where_clause=None, start_index=None, limit=None):
        """
        生成指定条件的SQL查询语句。
        """
        # 构造基本的SQL查询语句
        query_sql = f"SELECT {columns} FROM {table_mame}"
        if where_clause:
            query_sql += f" WHERE {where_clause}"
        # 如果指定了分页参数，则在SQL语句中添加LIMIT子句
        if start_index is not None and limit is not None:
            query_sql += f" LIMIT {limit} OFFSET {start_index}"
        return query_sql

    @handle_db_exception
    async def query(self, columns, table_mame, where_clause=None, start_index=None, batch_size=None, values=None):
        query_sql = self.gen_query_sql(columns, table_mame, where_clause, start_index, batch_size)
        async with self.pool.acquire() as conn:
            return await conn.fetch(query_sql, *values) if values else await conn.fetch(query_sql)

    @handle_db_exception
    async def query_in(self, columns, table_name, query_data: dict, extend: str = None):
        query_sql = f"SELECT {columns} FROM {table_name} "
        where_clause = "WHERE " + ' AND '.join([f"{k} IN {tuple(v)}" for k, v in query_data.items()])
        if extend:
            where_clause += " AND " + extend
        query_sql += where_clause

        async with self.pool.acquire() as conn:
            return await conn.fetch(query_sql)

    @handle_db_exception
    async def get_row_count(self, table_name, where_clause=None):
        sql = "SELECT COUNT(*) FROM " + table_name
        if where_clause:
            sql += " WHERE " + where_clause
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql)

    @handle_db_exception
    async def get_table_columns(self, table_name):
        query_sql = "SELECT column_name FROM information_schema.columns"
        if '.' in table_name:
            schema_name, table_name = table_name.split('.')
            query_sql += f" WHERE table_schema = '{schema_name}' AND TABLE_NAME = '{table_name}'"
        else:
            query_sql += f" WHERE TABLE_NAME = '{table_name}'"

        async with self.pool.acquire() as conn:
            return [col.get("column_name") for col in await conn.fetch(query_sql)]