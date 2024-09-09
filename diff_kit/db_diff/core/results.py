# @Project: diff-kit
# @Time: 2024/9/5 11:15
# @Author: Alan
# @File: results

from rich.table import Table


class Result:
    """比较结果"""

    def __init__(self, table_name_a=None, table_name_b=None, num_table_a=0, num_table_b=0) -> None:
        self.table_name_a = table_name_a
        self.table_name_b = table_name_b
        self.num_table_a = num_table_a
        self.num_table_b = num_table_b
        self.num_diff_row = 0
        self.only_row_count_in_table_a = 0
        self.excess_row_count_in_table_b = 0
        self.difference_row_count = 0
        self.only_rows_in_table_a = []
        self.excess_rows_in_table_b = []
        self.difference_rows = []

    @staticmethod
    def colorize_if(raw_string: str, condition: bool, color: str) -> str:
        # 设置命令行显示颜色
        return f"[{color}]{raw_string}[/{color}]" if condition else raw_string

    def get_summary_table(self, title: str = "对比结果") -> Table:
        """
        返回一个汇总结果的表格
        """
        table = Table(title=title, highlight=True)
        table.add_column("描述", justify="center", width=20)
        table.add_column("数量", justify="center", width=20)

        table.add_row("源表的总数量",
                      str(self.num_table_a))
        table.add_row("目标表的总数量",
                      str(self.num_table_b))
        table.add_row("此次对比数量",
                      str(self.num_diff_row))
        table.add_section()
        table.add_row("仅在源表的数量",
                      self.colorize_if(raw_string=str(self.only_row_count_in_table_a),
                                       condition=self.only_row_count_in_table_a > 0,
                                       color="red"))
        table.add_row("目标表存在多条的数量",
                      self.colorize_if(raw_string=str(self.excess_row_count_in_table_b),
                                       condition=self.excess_row_count_in_table_b > 0,
                                       color="red"))
        table.add_section()
        table.add_row("对比结果不同的数量",
                      self.colorize_if(raw_string=str(self.difference_row_count),
                                       condition=self.difference_row_count > 0,
                                       color="red"))
        return table

    def as_summary(self):
        return {
            "table_name_a": self.table_name_a,
            "table_name_b": self.table_name_b,
            "num_table_a": self.num_table_a,
            "num_table_b": self.num_table_b,
            "num_diff_row": self.num_diff_row,
            "only_row_count_in_table_a": self.only_row_count_in_table_a,
            "excess_row_count_in_table_b": self.excess_row_count_in_table_b,
            "difference_row_count": self.difference_row_count,
        }

    def get_only_rows_in_table_a(self):
        return self.only_rows_in_table_a

    def get_excess_rows_in_table_b(self):
        return self.excess_rows_in_table_b

    def get_difference_rows(self):
        return self.difference_rows

    def is_success(self):
        if self.num_table_a != self.num_table_b:
            return False
        return self.only_row_count_in_table_a == 0 \
            and self.excess_row_count_in_table_b == 0 \
            and self.difference_row_count == 0
