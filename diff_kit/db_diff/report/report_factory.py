# @Project: diff-kit
# @Time: 2024/9/4 17:16
# @Author: Alan
# @File: base

import os
from diff_kit.db_diff.core.results import Result
from diff_kit.db_diff.report.excel_report import ExcelReport
from diff_kit.db_diff.report.text_report import TextReport


def create_report_dir(filename):
    file_dir = os.path.join(os.getcwd(), 'output')
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    file_path = os.path.join(file_dir, filename)

    return file_path


class ReportFactory:

    @property
    def report_mapp(self):
        return {
            'excel': ExcelReport,
            'text': TextReport
        }

    def create_report(self, report_type: str, filename: str, result: Result, **kwargs):
        report_path = create_report_dir(filename)
        return getattr(self.report_mapp[report_type](), 'generate_report')(report_path=report_path, result=result, **kwargs)