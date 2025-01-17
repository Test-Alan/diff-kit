# @Project: diff-kit
# @Time: 2024/9/5 11:15
# @Author: Alan
# @File: __init__


from diff_kit.db_diff.core.compare_data import DbDiff, DiffParams, DbConfig
from diff_kit.db_diff.report.report_factory import ReportFactory


class DbDiffRunner:
    def __init__(self,
                 report_name: str,
                 diff_params: DiffParams,
                 only_generate_failed_report: bool = True,
                 report_type: str = 'excel',
                 ):
        """
        初始化对比数据库表的类实例。
        参数:
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
            maxsize: int = 20，协程数，默认为20个协程同时工作。
            compare_count: bool 是否只对比行数
        only_generate_failed_report: 是否仅失败时生成报告,
        report_type: str 报告类型，默认excel 可选['excel', 'text]
        plugin: callable 可自定义比较方法，默认为None
        """
        self.params = diff_params
        self.report_name = report_name
        self.only_generate_failed_report = only_generate_failed_report
        self.report_type = report_type

    def diff(self):
        """
        执行数据库对比并生成报告。
        """
        result = DbDiff(self.params).start()
        # 如果仅生成失败报告，并且对比结果为成功，则不生成报告
        if self.only_generate_failed_report and result.is_success():
            return True
        # 生成报告
        ReportFactory().create_report(self.report_type, self.report_name, result, sheet_name=self.params.table_name_a)

