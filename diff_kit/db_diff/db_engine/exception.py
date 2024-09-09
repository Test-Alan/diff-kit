# @Project: diff-kit
# @Time: 2024/9/4 16:40
# @Author: Alan
# @File: common

import functools

from diff_kit.utils.logger import logger


class DBException(Exception):
    """
    数据库异常基类
    """
    pass


def handle_db_exception(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        try:
            return await func(self, *args, **kwargs)
        except Exception as e:
            logger.error(f"执行参数：{args}, {kwargs}")
            raise DBException(e)
    return wrapper
