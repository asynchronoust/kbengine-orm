# -*- coding: utf-8 -*-
"""
FileName:   columns
Author:     Tao Hao
@contact:   taohaohust@outlook.com
Created time:   2018/10/21

Description:

Changelog:
"""
import pickle
import json

_escape_table = [chr(x) for x in range(128)]
_escape_table[0] = u'\\0'
_escape_table[ord('\\')] = u'\\\\'
_escape_table[ord('\n')] = u'\\n'
_escape_table[ord('\r')] = u'\\r'
_escape_table[ord('\032')] = u'\\Z'
_escape_table[ord('"')] = u'\\"'
_escape_table[ord("'")] = u"\\'"


def escape_string(value, mapping=None):
    """escapes *value* without adding quote.

    Value should be unicode
    将mysql中的特殊字符进行转义存进去，避免被当成了mysql的字符特殊含义而不是字符串本身的含义
    """
    # str.translate 将value中的ascii对应的数字的index
    # 替换成_escape_table这个列表的index对应的字符
    return value.translate(_escape_table)


DB_NULL = "NULL"


class ColumnBase(object):

    __blob__ = False

    @staticmethod
    def loads(v):
        raise NotImplementedError

    @staticmethod
    def dumps(v):
        raise NotImplementedError


class STRING(ColumnBase):

    @staticmethod
    def loads(v):
        """
        v is bytes
        """
        if not v:
            return ""

        return str(v, encoding="utf8")

    @staticmethod
    def dumps(v):
        return escape_string(str(v))


class LIST(ColumnBase):

    __blob__ = True

    @staticmethod
    def loads(v):
        # 如果数据库中是NULL，则value是python的None
        if not v:
            return []
        else:
            return pickle.loads(v)

    @staticmethod
    def dumps(v):
        if not v:
            return DB_NULL

        return pickle.dumps(v)


class DICT(ColumnBase):

    __blob__ = True

    @staticmethod
    def loads(v):
        # 如果数据库中是NULL，则value是python的None
        if not v:
            return {}
        else:
            return pickle.loads(v)

    @staticmethod
    def dumps(v):
        if not v:
            return DB_NULL

        return pickle.dumps(v)


class JSON(ColumnBase):

    @staticmethod
    def loads(v):
        if not v:
            return None
        else:
            return json.loads(str(v, encoding="utf8"))

    @staticmethod
    def dumps(v):
        if not v:
            return DB_NULL
        else:
            # 需要escape_string 去转一下
            return escape_string(json.dumps(v))


class INT(ColumnBase):

    @staticmethod
    def loads(v):
        return int(v)

    @staticmethod
    def dumps(v):
        if v == DB_NULL:
            return DB_NULL

        return int(v)


class FLOAT(ColumnBase):

    @staticmethod
    def loads(v):
        return float(v)

    @staticmethod
    def dumps(v):
        if v == DB_NULL:
            return 0.0

        return float(v)


if __name__ == "__main__":
    a = JSON
    print(isinstance(a, JSON))
    print(issubclass(a, JSON))
    print(issubclass(a, (ColumnBase, JSON)))

