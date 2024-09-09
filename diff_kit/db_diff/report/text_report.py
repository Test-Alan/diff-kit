# @Project: diff-kit
# @Time: 2024/9/4 17:30
# @Author: Alan
# @File: text_report

from pathlib import Path
from diff_kit.db_diff.core.results import Result


class TextReport(object):
    def generate_txt_report(self, report_path: Path, result: Result):
        """
        生成text报告
        """
        with open(report_path, 'w', encoding='utf-8') as file:
            file.write(
                f"比较两个表:\n"
                f"表A为: {result.table_name_a}\n"
                f"表B为: {result.table_name_b}\n")
            file.write(f"表A总记录数: {result.num_table_a}\n")
            file.write(f"表B总记录数: {result.num_table_b}\n")
            file.write(f"此次对比记录数: {result.num_diff_row}\n")
            file.write(f"表A仅有的记录数: {result.only_row_count_in_table_a}\n")
            file.write(f"表B多出的记录数: {result.excess_row_count_in_table_b}\n")
            file.write(f"表A和表B结果不同的记录数: {result.difference_row_count}\n")

            for i in result.difference_rows:
                file.write(f"表A和表B结果不同的记录: {i}\n")

            for i in result.only_rows_in_table_a:
                file.write(f"表A仅有的记录: {i}\n")

            for i in result.excess_rows_in_table_b:
                file.write(f"表B多出的记录: {i}\n")
