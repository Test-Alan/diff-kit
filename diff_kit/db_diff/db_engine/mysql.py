# @Project: diff-kit
# @Time: 2024/9/2 13:31
# @Author: Alan
# @File: mysql


import aiomysql

from diff_kit.db_diff.db_engine.abc import DbEngine
from diff_kit.db_diff.db_engine.exception import handle_db_exception


class AsyncMysqlDbEngine(DbEngine):

    def __init__(self, host, port, user, password, db):
        super().__init__(host, port, user, password, db)

    async def create_pool(self, maxsize=10):
        self.pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db,
            maxsize=maxsize
        )

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()

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
            query_sql += f" LIMIT {start_index}, {limit}"
        return query_sql

    @handle_db_exception
    async def query_generator(self, columns, table_mame, where_clause=None, start_index=None, batch_size=None):
        query_sql = self.gen_query_sql(columns, table_mame, where_clause, start_index, batch_size)
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query_sql)
                async for row in cur:
                    yield row

    @handle_db_exception
    async def query(self, columns, table_mame, where_clause=None, start_index=None, batch_size=None, values=None):
        query_sql = self.gen_query_sql(columns, table_mame, where_clause, start_index, batch_size)
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query_sql, values)
                return await cur.fetchall()

    @handle_db_exception
    async def query_in(self, columns, table_name, query_data: dict) -> dict:
        query_sql = f"SELECT {columns} FROM {table_name} "
        where_clause = "WHERE " + ' AND '.join([f"{k} IN %s" for k in query_data.keys()])
        values = [tuple(v) for v in query_data.values()]
        query_sql += where_clause
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query_sql, values)
                return await cur.fetchall()

    @handle_db_exception
    async def get_row_count(self, table_name, where_clause=None):
        query = f"SELECT COUNT(*) FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                results = await cur.fetchone()
                row_count = results[0]
                return row_count

    @handle_db_exception
    async def get_table_columns(self, table_name):
        query = f"SHOW COLUMNS FROM {table_name}"
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                columns = [column[0] for column in await cur.fetchall()]
                return columns
