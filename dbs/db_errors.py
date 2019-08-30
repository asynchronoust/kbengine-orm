# -*- coding: utf-8 -*-
"""
FileName:   db_errors
Author:     Tao Hao
@contact:   taohaohust@outlook.com
Created time:   2018/10/29

Description:

Changelog:
"""


class BaseDbError(object):

    def __str__(self):
        return "base db error"


class DbDeleteErrorNotFound(BaseDbError):

    # 因为kbengine返回的db error是一个字符串，这里用类来区分一下吧
    # 目前也可以将此类当做str来用，复写__str__函数
    # 在 print 中 使用 %s 格式化字符串的时候直接用 __str__的内容

    def __str__(self):
        return "no rows found to delete"


class DbUpdateErrorNotFound(BaseDbError):

    def __str__(self):
        return "no rows found to update"



