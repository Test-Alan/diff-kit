# @Project: diff-kit
# @Time: 2024/9/5 11:15
# @Author: Alan
# @File: compare_data

import asyncio
import time
import dictdiffer
from pydantic import BaseModel
from rich.console import Console
from collections import defaultdict
from tqdm.asyncio import tqdm as tqdm_async
from typing import Union, List, Dict, Any, Optional, Callable
from diff_kit.db_diff.core.results import Result
from diff_kit.db_diff.db_engine import DbEngineFactory
from diff_kit.utils.logger import logger

console = Console()


class DbConfig(BaseModel):
    db_type: str
    host: str
    port: int
    user: str
    password: str


class DiffParams(BaseModel):
    db_conn_a: DbConfig
    db_conn_b: Optional[DbConfig] = None
    db_name_a: str
    db_name_b: str
    table_name_a: str
    table_name_b: str
    where_clause_a: Optional[str] = None
    where_clause_b: Optional[str] = None
    field_mapping: Optional[dict] = None
    unique_field: Union[list, dict]
    diff_columns: Optional[list] = None
    exclude_columns: Optional[list] = None
    limit: int = 1000
    maxsize: int = 50
    compare_count: bool = False
    fast: bool = True
    plugin: Optional[Callable] = None


class DbDiff:

    def __init__(self, kwargs: DiffParams):
        self.kwargs = kwargs
        self.client_a = None
        self.client_b = None
        self.query_columns_a = "*"
        self.query_columns_b = "*"
        self.results = None

    async def create_db_conn(self):
        """
        创建数据库连接
        """
        # 如果db_conn_b没有提供，就使用db_conn_a值

        if not self.kwargs.db_conn_b:
            self.kwargs.db_conn_b = self.kwargs.db_conn_a
        # 如果db_name_b没有提供，就使用db_name_a的值
        if not self.kwargs.db_name_b:
            self.kwargs.db_name_b = self.kwargs.db_name_a

        try:
            db_conn_config_a = self.kwargs.db_conn_a.model_dump(exclude={'db_type'})
            db_conn_config_b = self.kwargs.db_conn_b.model_dump(exclude={'db_type'})
            self.client_a = DbEngineFactory(
                self.kwargs.db_conn_a.db_type,
                db=self.kwargs.db_name_a,
                **db_conn_config_a
            ).create_db_engine()
            self.client_b = DbEngineFactory(
                self.kwargs.db_conn_b.db_type,
                db=self.kwargs.db_name_b,
                **db_conn_config_b
            ).create_db_engine()
            # 创建数据库连接池
            await self.client_a.create_pool(self.kwargs.maxsize)
            await self.client_b.create_pool(self.kwargs.maxsize)
        except Exception as e:
            raise RuntimeError("Failed to create database connections: {}".format(e))

    async def close_db_conn(self):
        await self.client_a.close()
        await self.client_b.close()

    def _handle_query_columns(self, columns, method='alias'):
        """
        处理不同的列名。
        此方法根据提供的处理方式（默认为别名(alias)），对差异列名进行处理。
        如果处理方式为'alias'，则在列名后添加as xxx（如果列名在映射中存在）；
        如果处理方式为'replace'，则直接用映射中的值替换列名（如果存在映射）。

        @param method: 处理方式，默认为'alias'。可选值为'alias'和'replace'。
        @return: 处理后的列名列表。
        """
        field_mapping = self.kwargs.field_mapping
        # 判断映射是否为空，如果为空，直接返回差异列列表
        if field_mapping is None:
            return columns

        # 检查处理方式是否有效
        if method not in ['alias', 'replace']:
            raise ValueError(f'{method} 必须是alias或replace')

        # 为每个列名添加as别名（如果在映射中存在）
        if method == 'alias':
            return [col + f' as {field_mapping.get(col)}' if field_mapping.get(col) else col for col in columns]

        # 使用映射中的值替换列名（如果存在映射）
        if method == 'replace':
            return [field_mapping.get(col, col) for col in columns]

    def _join_query_columns(self, query_columns: list):
        """
        拼接并查询列
        参数:
        - query_columns: list，包含需要查询的列名的列表。

        返回值:
        - str，拼接后的列名字符串，各列名之间以逗号分隔。
         [id, name, age] => id, name, age
        """
        exclude_columns = self.kwargs.exclude_columns or []
        return ','.join(filter(lambda x: x not in exclude_columns, query_columns))

    def _join_where_clause(self, where_clause: list):
        """
        拼接查询条件
        """
        return ' AND '.join(where_clause)

    def _parse_where_clause_b(self, row_a: dict):
        # 根据字段值的类型生成相应的查询条件字符串。
        def check_type(k, v, sign='='):
            if isinstance(v, int) or isinstance(v, float):
                return f"{k} {sign} {v}"
            if v is None:
                return f"{k} IS NULL"
            if v == '':
                return f"{k} {sign} ''"

            return f"{k} {sign} '{str(v)}'"

        if isinstance(self.kwargs.unique_field, dict):
            expression = [check_type(field_b, row_a[field_a]) for field_a, field_b in self.unique_field.items()]

        elif isinstance(self.kwargs.unique_field, list):
            expression = [check_type(field, row_a[field]) for field in self.kwargs.unique_field]

        else:
            raise TypeError("unique_field must be a dict or a list")

        if self.kwargs.where_clause_b:
            expression.append(self.kwargs.where_clause_b)

        return ' AND '.join(expression)

    def _parse_query_condition_in_b(self, result_a):
        """
        根据查询条件解析来自结果A的条件并生成适用于B的批量查询条件

        此函数的作用是根据预设的查询条件(self.where_clause_b)将结果A(list of dicts)中的特定字段
        提取出来，形成一个新的查询条件字典(batch_where_clause_b)，该字典用于后续对数据源B的查询。
        同时，该函数会返回一个包含所有查询键的列表(keys)。

        """
        batch_where_clause_b, keys_a, keys_b = {}, [], []

        def add_batch_where_clause(ka, kb, values):
            batch_where_clause_b[kb] = [str(row_a[ka]) if row_a[ka] else row_a[ka] for row_a in values]
            keys_a.append(ka)
            keys_b.append(kb)

        # 确认where_clause_b是字典还是列表
        if isinstance(self.kwargs.unique_field, dict):
            for ka, kb in self.kwargs.unique_field.items():
                add_batch_where_clause(ka, kb, result_a)

        # 如果查询条件是列表，遍历其元素
        elif isinstance(self.kwargs.unique_field, list):
            for k in self.kwargs.unique_field:
                add_batch_where_clause(k, k, result_a)
        else:
            raise TypeError("unique_field must be a dict or a list")

        return keys_a, keys_b, batch_where_clause_b

    def _generate_key(self, row: Dict[str, Any], keys: List[str]) -> str:
        """
        生成用于唯一标识行的键。
        """
        return '_'.join([str(row[k]) for k in keys])

    def _build_results_dict(self, query_b_result: List[Dict[str, Any]], keys: List[str]) -> Dict[
        str, List[Dict[str, Any]]]:
        """
        构建用于存储查询结果字典。
        """
        results_dict = defaultdict(list)
        for row in query_b_result:
            key = self._generate_key(row, keys)
            results_dict[key].append(row)
        return results_dict

    async def get_row_count(self, is_use_query_condition=False):
        """
        获取表的总行数
        :param is_use_query_condition: 是否使用查询条件
        :return:
        """
        where_clause_a = self.kwargs.where_clause_a
        where_clause_b = self.kwargs.where_clause_b

        # 如果不使用查询条件，则将查询条件置为None
        if not is_use_query_condition:
            where_clause_a = None
            where_clause_b = None

        return await asyncio.gather(
            self.client_a.get_row_count(self.kwargs.table_name_a, where_clause_a),
            self.client_b.get_row_count(self.kwargs.table_name_b, where_clause_b)
        )

    async def distribute_run_tasks(self, task_name, total_tasks, batch_size, maxsize):
        """
        分配任务
        :param task_name: 任务名称
        :param total_tasks: 任务总数
        :param batch_size: 每次只执行大小
        """
        num_batches = (total_tasks // batch_size) + (total_tasks % batch_size > 0)
        # 控制并发数
        semaphore = asyncio.Semaphore(maxsize)

        async def task(pbar, start_index, batch_size):
            async with semaphore:
                if self.kwargs.fast:
                    await self.compare_data_batch(pbar, start_index, batch_size)
                else:
                    await self.compare_data(pbar, start_index, batch_size)

        with tqdm_async(total=total_tasks, desc=task_name, unit="row", ncols=160) as pbar:
            tasks = [task(pbar, start_index * batch_size, batch_size)
                     for start_index in range(num_batches)]

            await asyncio.gather(*tasks)

    async def run_compare(self):
        try:
            # 创建数据库连接
            await self.create_db_conn()
            # 只对比行数
            if self.kwargs.compare_count:
                return await self.compare_row_count()
            # 对比数据
            return await self.batch_compare_data()
        except Exception as e:
            # 记录执行过程中的异常
            logger.error(f"Error occurred: {e}")
            raise RuntimeError(f"Error occurred: {e}")
        finally:
            # 确保与数据库的连接被正确关闭
            await self.close_db_conn()

    async def compare_row_count(self):
        row_count_a, row_count_b = await self.get_row_count(is_use_query_condition=True)
        # 初始化结果
        self.results = Result(self.kwargs.table_name_a, self.kwargs.table_name_b, num_table_a=row_count_a,
                              num_table_b=row_count_b)
        return self.results

    async def batch_compare_data(self):
        """
        批量对比数据的异步方法。
        """
        # 如果未指定对比的列，则自动获取基础表A的列信息
        diff_columns = self.kwargs.diff_columns or await self.client_a.get_table_columns(self.kwargs.table_name_a)

        # 处理差异列，使用别名方法
        self.query_columns_a = self._join_query_columns(self._handle_query_columns(diff_columns, method='alias'))
        self.query_columns_b = self._join_query_columns(self._handle_query_columns(diff_columns, method='replace'))

        # 获取基础表A和对比表B的行数，并根据查询条件获取A表中不同行的数
        table_a_total_num, table_b_total_num = await self.get_row_count(is_use_query_condition=False)
        # 初始化结果
        self.results = Result(self.kwargs.table_name_a, self.kwargs.table_name_b, num_table_a=table_a_total_num,
                              num_table_b=table_b_total_num)

        diff_row_count = table_a_total_num
        if self.kwargs.where_clause_a:
            diff_row_count = await self.client_a.get_row_count(self.kwargs.table_name_a, self.kwargs.where_clause_a)

        name = f"Task 比较两个表，基础表为: {self.kwargs.table_name_a}, 对比表为: {self.kwargs.table_name_b}"
        await self.distribute_run_tasks(name, diff_row_count, self.kwargs.limit, self.kwargs.maxsize)

        return self.results

    async def compare_data_batch(self, pbar, start_index, batch_size):
        # 查询A表数据
        query_a_result = await self.client_a.query(
            self.query_columns_a,
            self.kwargs.table_name_a,
            self.kwargs.where_clause_a,
            start_index,
            batch_size
        )
        # 根据A表数据解析出B表的查询条件
        keys_a, keys_b, batch_where_clause_b = self._parse_query_condition_in_b(query_a_result)
        # 查询B表数据
        query_b_result = await self.client_b.query_in(
            self.query_columns_b,
            self.kwargs.table_name_b,
            batch_where_clause_b,
            self.kwargs.where_clause_b
        )

        # 将数据源B的查询结果转换为字典形式
        query_b_result_dict = self._build_results_dict(query_b_result, keys_b)
        # 遍历数据源A的查询结果
        for row_a in query_a_result:
            if self.kwargs.plugin:
                row_a = self.kwargs.plugin(row_a)
            # 生成数据源B的查询条件
            where_clause_b = ' AND '.join([key + '=' + str(row_a[key]) for key in keys_a])
            # 生成B表结果对应的key
            key_b = self._generate_key(row_a, keys_b)
            # 根据条件从数据源B的结果字典中获取对应行
            row_b = query_b_result_dict[key_b]
            # 比较来自两个数据源的行，并更新结果
            await self.compare_row(row_a, row_b, where_clause_b)
            # 更新进度条
            pbar.update(1)

        del query_a_result
        del query_b_result_dict
        del batch_where_clause_b

    async def compare_data(self, pbar, start_index, batch_size):
        """
        异步比较两个数据源中指定条件的数据批。
        :param start_index: 数据起始索引，用于分批处理数据。
        :param batch_size: 批处理大小，即每次查询的数据量。
        :return: 返回一个结果报告，包含比较中发现的不同行数。
        """
        async for row_a in self.client_a.execute_query_generator(
                self.query_columns_a,
                self.kwargs.table_name_a,
                self.kwargs.where_clause_a,
                start_index,
                batch_size
        ):
            where_clause_b = self._parse_where_clause_b(row_a)
            row_b = await self.client_b.execute_query(self.query_columns_b, self.table_name_b, where_clause_b)
            await self.compare_row(row_a, row_b, where_clause_b)
            pbar.update(1)

    async def compare_row(self, row_a, row_b, where_clause_b):
        """
        异步比较两个表中指定行的数据差异，并将差异信息更新到报告中。
        """
        self.results.num_diff_row += 1

        # 如果表B中没有找到匹配的记录，则更新报告中仅存在于表A的记录计数和列表，并记录警告日志。
        if not row_b:
            self.results.only_row_count_in_table_a += 1
            self.results.only_rows_in_table_a.append(where_clause_b)
            # logger.warning(f"在{self.table_name_b}未找到匹配的记录: {where_clause_b}")

        # 如果表B中找到多个匹配的记录，则更新报告中在表B中有更多记录的计数和列表，并记录警告日志，同时比较差异。
        elif len(row_b) > 1:
            self.results.excess_row_count_in_table_b += 1
            self.results.excess_rows_in_table_b.append(where_clause_b)
            # logger.warning(f"在{self.table_name_b}找到多条匹配记录: {where_clause_b}")

        # 如果表B中找到一个匹配的记录，则进行差异比较。
        else:
            # 获取表B中与row_a匹配的行。
            row_b = row_b[0]
            # 使用dictdiffer库比较两行数据的差异，并将差异结果转化为列表。
            differences_generator = dictdiffer.diff(dict(row_a), dict(row_b), ignore=None)
            differences = list(differences_generator)
            # 如果存在差异，则更新报告中差异计数和差异列表，并记录警告日志。
            if len(differences) > 0:
                self.results.difference_row_count += 1
                self.results.difference_rows.append((where_clause_b, differences))
                # logger.warning(f"结果有差异: {differences}")

    def start(self):
        start_time = time.perf_counter()
        result = asyncio.run(self.run_compare())
        # 在控制台打印汇总结果的表格
        console.print()
        console.print(result.get_summary_table())
        console.print()
        logger.info(f"消耗时间: {time.perf_counter() - start_time}")
        return result
