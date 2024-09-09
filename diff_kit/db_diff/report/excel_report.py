# @Project: mongo-diff
# @Time: 2024/8/6 16:12
# @Author: Alan
# @File: report

import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from diff_kit.db_diff.core.results import Result
from diff_kit.utils.logger import logger

COLUMN_DESC = "描述"
COLUMN_NUM = "数量"
COLUMN_QUERY = "查询条件"
COLUMN_INCONSISTENT_FIELDS = "不一致字段"
COLUMN_A_VALUE = "源表的值"
COLUMN_B_VALUE = "目标表的值"


class ExcelReport:

    def generate_report(self, sheet_name: str, report_path: Path, result: Result):
        """
        生成Excel报告
        """
        sheet_name = sheet_name[:29]
        row_gap = 2
        suffix = ".xlsx"
        logger.info("生成报告中...")
        if suffix not in str(report_path):
            report_path += suffix

        def align_left(data_frame):
            return data_frame.style.apply(lambda x: ['text-align: left' for _ in x], axis=0)

        def add_summary(writer, summary, start_row):
            if not summary:
                return
            df_summary = pd.DataFrame(summary)
            align_left(df_summary).to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row)

        # 获取数据
        summary_values = list(result.as_summary().values())
        difference_rows = result.get_difference_rows()
        only_rows_in_table_a = result.get_only_rows_in_table_a()
        excess_rows_in_table_b = result.get_excess_rows_in_table_b()

        # 起始行数
        differ_across_start_row = len(summary_values) + row_gap
        table_a_only_start_row = len(summary_values) + row_gap
        table_b_more_start_row = len(summary_values) + row_gap

        try:
            if not os.path.exists(report_path):
                with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
                    writer.book.create_sheet(title=sheet_name)
            else:
                sheet_name = f"{sheet_name}_{len(pd.ExcelFile(report_path).sheet_names)}"

            with pd.ExcelWriter(report_path, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:

                # 概述信息
                summary = {
                    COLUMN_DESC: ['源表名', '目标表名', '源表总记录数', '目标表总记录数', '此次对比记录数',
                                  '仅在源表的记录数',
                                  '目标表存在多条的记录数', '对比不一致的记录数'],
                    COLUMN_NUM: summary_values
                }
                add_summary(writer, summary, 0)

                # 表A与表B不一致的记录
                if difference_rows:

                    differ_across_summary = {
                        COLUMN_DESC: [],
                        COLUMN_QUERY: [],
                        COLUMN_INCONSISTENT_FIELDS: [],
                        COLUMN_A_VALUE: [],
                        COLUMN_B_VALUE: []
                    }

                    def add_column(query_criteria, field, value1, value2):
                        differ_across_summary[COLUMN_DESC].append('对比不一致的记录')
                        differ_across_summary[COLUMN_QUERY].append(query_criteria)
                        differ_across_summary[COLUMN_INCONSISTENT_FIELDS].append(field)
                        differ_across_summary[COLUMN_A_VALUE].append(value1)
                        differ_across_summary[COLUMN_B_VALUE].append(value2)

                    for differ_across_tables in difference_rows:
                        query_criteria, differ_across = differ_across_tables
                        for item in differ_across:
                            if "remove" in item:
                                _, _, values = item
                                for field, value in values:
                                    add_column(query_criteria, field, self.format_value(values), "remove")
                            elif "add" in item:
                                _, _, values = item
                                for field, value in values:
                                    add_column(query_criteria, field, "add", self.format_value(value))
                            else:
                                _, field, values = item
                                add_column(query_criteria, field, self.format_value(values[0]),
                                           self.format_value(values[1]))

                    add_summary(writer, differ_across_summary, differ_across_start_row)
                    table_a_only_start_row = differ_across_start_row + len(differ_across_summary[COLUMN_DESC]) + row_gap

                # 仅在A表的记录
                if only_rows_in_table_a:
                    table_a_only_summary = {
                        COLUMN_DESC: ['仅在源表的记录' for _ in only_rows_in_table_a],
                        COLUMN_QUERY: only_rows_in_table_a
                    }
                    add_summary(writer, table_a_only_summary, table_a_only_start_row)
                    table_b_more_start_row = table_a_only_start_row + len(table_a_only_summary[COLUMN_DESC]) + row_gap

                # B表多条的数据
                if excess_rows_in_table_b:
                    table_b_more_summary = {
                        COLUMN_DESC: ['目标表存在多条的记录' for _ in excess_rows_in_table_b],
                        COLUMN_QUERY: excess_rows_in_table_b
                    }
                    add_summary(writer, table_b_more_summary, table_b_more_start_row)

        except Exception as e:
            logger.error(f"生成报告时发生错误: {e}")
        else:
            logger.info(f"报告生成完成：{report_path}")

    @staticmethod
    def format_value(value):
        """
        格式化值为统一的字符串格式。

        :param value: 需要格式化的值，可以是datetime、int、float、str或None。
        :return: 格式化后的字符串。
        """
        if isinstance(value, datetime):
            if '.' in str(value):
                value = value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            return f'"{str(value)}"'

        if isinstance(value, (int, float)):
            return str(value)

        if value is None:
            return "null"

        if value == '':
            return '""'

        return value
