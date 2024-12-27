# 对比工具集

## 一. 项目介绍

做一个快速的对比工具集

### 目前实现功能
1. 数据库对比
    - mysql
    - pgsql

# 二. 数据库对比
### 1. 安装
```shell
pip install diff-kit
```
### 2. 执行
```python
from diff_kit.db_diff.core import DbDiffRunner
"""
参数说明：
  report_name: str 报告名称
  diff_params:
      db_conn_a: dict 数据库连接信息
          db_type: str 数据库类型，目前支持[mysql, pgsql]
          host: str 数据库连接地址
          port: int 数据库连接端口
          user: str 数据库连接用户名
          password: str 数据库连接密码
      db_conn_b: dict 数据库连接信息
      db_name_a: str，数据库A的名称。
      table_name_a: str，表A的名称。
      table_name_b: str，表B的名称。
      db_name_b: str = None，数据库B的名称，默认为None。
      where_clause_a: str = None，查询条件A，为字符串格式，默认为None。
      where_clause_b: dict = None，查询条件B，为字典格式，默认为None。
      diff_columns: list = None，需要进行差异比较的列名列表，默认为None。
      exclude_columns: list = None，需要排除在比较之外的列名列表，默认为None。
      field_mapping: dict = None，列名映射字典，用于指定表A的列名如何映射到表B的列名，默认为None。
      unique_field: Union[list, dict] A表与B表关联的唯一字段
      limit: int = 1000，查询限制的数量，默认为1000条。
      maxsize: int = 20，协程数，默认为20个协程任务同时工作。
      compare_count: bool 是否只对比行数
  only_generate_failed_report: 是否仅失败时生成报告,
  report_type: str 报告类型，默认excel 可选['excel', 'text]
  plugin: callable 可自定义比较方法，默认为None
"""
params = {
    "report_name": "demo",
    "db_conn_a": {"db_type": "mysql","host": "127.0.0.1", "port": 3306, "user": "root", "password": "123456"},
    "db_conn_b": {"db_type": "mysql","host": "127.0.0.1", "port": 3306, "user": "root", "password": "123456"},
    "db_name_a": "test",
    "table_name_a": "test_table1",
    "table_name_b": "test_table2",
    "unique_field": ["id"],
}
DbDiffRunner(**params).diff()
```

### 3. 查看报告
报告在output目录下
![img.png](img.png)