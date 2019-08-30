# -*- coding: utf-8 -*-
"""
FileName:   base
Author:     Tao Hao
@contact:   taohaohust@outlook.com
Created time:   2018/9/25

Description:

Changelog:
"""
import KBEngine
from Functor import Functor
from KBEDebug import *
from dbs.columns import STRING, JSON, INT, escape_string
import pickle
from dbs import db_errors
import functools


ORDER_DESC = "desc"
ORDER_ASC = "asc"


def db_op(func):
    """
    db model中操作数据的一些检查清理工作
    """

    @functools.wraps(func)
    def wrap(*args, **kwargs):
        this = args[0]
        func(*args, **kwargs)
        this.clear()

    return wrap


def _escape_string(func):

    def wrapper(*args, **kwargs):
        self = args[0]
        key = args[1]
        value = args[2]
        if self.model.__fields__.get(key) == STRING:
            if isinstance(value, str):
                value = escape_string(value)
            elif isinstance(value, list):
                value = [escape_string(x) for x in value]

        ret = func(self, key, value, **kwargs)
        return ret

    return wrapper


class BaseModel(object):
    __table__ = ""
    __fields__ = {}
    __split_num__ = 0

    def __init__(self):
        if not self.__table__ or not self.__fields__:
            ERROR_MSG("BaseModel. __table__ or __fields__ is empty")
            raise Exception

        self.dml = DML(self)

    def get_table(self, split_value):
        if self.__split_num__:
            mod_ = split_value % self.__split_num__
            return "%s_%s" % (self.__table__, mod_)
        else:
            return self.__table__


class DML(object):

    def __init__(self, model):
        self.model = model

        # 使用列表，保证顺序是按照调用函数的顺序去数据库中进行过滤
        # 需要使用索引的字段，需要先调用函数
        self._filters = []
        self._not_filters = []
        self._in_filters = []
        self._gt_filters = []
        self._gte_filters = []
        self._lt_filters = []
        self._lte_filters = []
        self._orders = []
        self._limit = 0

    def clear(self):
        self._filters = []
        self._not_filters = []
        self._in_filters = []
        self._gt_filters = []
        self._gte_filters = []
        self._lt_filters = []
        self._lte_filters = []
        self._orders = []
        self._limit = 0

    def _get_table(self, table):
        return table or self.model.__table__

    def _get_filter_cmp_phase(self, cmp_operator, key, value):
        key_type = self.model.__fields__.get(key)
        if key_type == STRING:
            return "%s%s'%s'" % (key, cmp_operator, value)
        else:
            return "%s%s%s" % (key, cmp_operator, value)

    def _get_filter_phase(self):
        filter_phase = []
        if self._filters:
            for key, value in self._filters:
                filter_phase.append(self._get_filter_cmp_phase("=", key, value))

        if self._not_filters:
            for key, value in self._not_filters:
                filter_phase.append(self._get_filter_cmp_phase("!=", key, value))

        if self._in_filters:
            for key, value in self._in_filters:
                filter_phase.append("%s in %s" % (key, value))

        if self._gte_filters:
            for key, value in self._gte_filters:
                filter_phase.append(self._get_filter_cmp_phase(">=", key, value))

        if self._gt_filters:
            for key, value in self._gt_filters:
                filter_phase.append(self._get_filter_cmp_phase(">", key, value))

        if self._lte_filters:
            for key, value in self._lte_filters:
                filter_phase.append(self._get_filter_cmp_phase("<=", key, value))

        if self._lt_filters:
            for key, value in self._lt_filters:
                filter_phase.append(self._get_filter_cmp_phase("<", key, value))

        return filter_phase

    @staticmethod
    def _gen_filter_phase(filter_phase):
        return " and ".join(filter_phase)

    def _get_order_phase(self):
        phase_list = []
        for k, direction in self._orders:
            phase_list.append("%s %s" % (k, direction))

        return " order by %s" % ",".join(phase_list)

    @_escape_string
    def eq(self, key, value):
        """
        ==，需要使用索引的key需要先调用
        """
        self._filters.append((key, value))
        return self

    @_escape_string
    def neq(self, key, value):
        """
        !=
        """
        self._not_filters.append((key, value))
        return self

    @_escape_string
    def gt(self, key, value):
        """
        >
        """
        self._gt_filters.append((key, value))
        return self

    @_escape_string
    def gte(self, key, value):
        """
        >=
        """
        self._gte_filters.append((key, value))
        return self

    @_escape_string
    def lt(self, key, value):
        """
        <
        """
        self._lt_filters.append((key, value))
        return self

    @_escape_string
    def lte(self, key, value):
        """
        <=
        """
        self._lte_filters.append((key, value))
        return self

    def order_by(self, key, direction):
        """
        :param key: 排序的key
        :param direction: 使用常量 ORDER_DESC 和 ORDER_ASC
        """
        self._orders.append((key, direction))
        return self

    def limit(self, num):
        self._limit = num

    def or_(self):
        """
        注意 OR 可能使整个语句使用不了索引，要谨慎使用
        """
        pass

    @_escape_string
    def in_(self, key, values):
        """
        in 操作如果values中的某些值不存在的话，不会使用索引
        :param key:
        :param values: 是一个list
        """
        key_type = self.model.__fields__.get(key)
        if key_type not in (STRING, INT):
            ERROR_MSG("key type not be set to in_ phase")
            return

        if key_type == STRING:
            self._in_filters.append(
                (key, "(%s)" % ",".join(["'%s'" % str(v) for v in values]))
            )
        else:
            self._in_filters.append(
                (key, "(%s)" % ",".join([str(v) for v in values]))
            )
        return self

    @db_op
    def insert(self, data, cb=None, table=None, dup_key_update=False,
               update_data=None, thread_id=None):
        """
        insert data中的数据
        :param thread_id:
        :param update_data: 在 dup_key_update=True时，此值有效，表示 upsert的值
        :param dup_key_update: DUPLICATE KEY UPDATE，在主键冲突的时候update
                            用来实现 upsert
        :param data:
        :param table:
        :param cb: 回调，它有两个参数 (insert_id, error)
        """
        _table = self._get_table(table)
        field_keys = list(data.keys())
        field_keys_format = []
        for key in field_keys:
            field_type = self.model.__fields__.get(key)
            data[key] = field_type.dumps(data[key])   # insert的时候需要dumps一下字段的值

            # 字符串需要带个引号包起来
            if issubclass(field_type, (STRING, JSON)):
                DEBUG_MSG("field key: %s" % key)
                field_keys_format.append("'{%s}'" % key)
            else:
                field_keys_format.append("{%s}" % key)

        DEBUG_MSG("field_keys_format: %s" % str(field_keys_format))

        sql = "INSERT INTO {table} ({fields}) VALUES ({fields_value})".format(
            table=_table,
            fields=",".join(field_keys),
            fields_value=",".join(field_keys_format)
        ).format(**data)

        if dup_key_update:
            update_key = list(update_data.keys())
            field_keys_format = []
            for key in update_key:
                key_type = self.model.__fields__.get(key)
                if issubclass(key_type, (STRING, JSON)):
                    field_keys_format.append("'{%s}'" % key)
                else:
                    field_keys_format.append("{%s}" % key)

            update_str_format = ",".join(
                "%s=%s" % (key, field_keys_format[i])
                for i, key in enumerate(update_key)
            )

            update_str = update_str_format.format(**update_data)
            sql = "%s ON DUPLICATE KEY UPDATE %s" % (sql, update_str)

        DEBUG_MSG("DML::insert, sql[%s]" % sql)
        if thread_id is None:
            KBEngine.executeRawDatabaseCommand(
                sql, Functor(self._insert_cb, cb, data, _table, sql)
            )
        else:
            KBEngine.executeRawDatabaseCommand(
                sql, Functor(self._insert_cb, cb, data, _table, sql), thread_id
            )

    def _insert_cb(self, cb, data, table, sql, result, rows, insertid, error):
        if error is not None:
            ERROR_MSG("DML::_insert_cb, insert db error. "
                      "data[%s], table[%s], sql[%s], error[%s]" %
                      (str(data), table, sql, error))

        if cb:
            cb(insertid, error)

    @db_op
    def insert_many(self, datas, cb=None, table=None):
        """
        insert 多个
        :param cb: 回调，它有两个参数 (insert_id, error)
        :param table: 可选的表名
        :param datas: 是一个 list
        """
        _table = self._get_table(table)
        if not datas:
            WARNING_MSG("insert_many, datas is none. return")
            return
        data = datas[0]
        field_keys = list(data.keys())
        field_keys_format = []
        for key in field_keys:
            field_type = self.model.__fields__.get(key)
            # 字符串需要带个引号包起来
            if issubclass(field_type, (STRING, JSON)):
                DEBUG_MSG("field key: %s" % key)
                field_keys_format.append("'{%s}'" % key)
            else:
                field_keys_format.append("{%s}" % key)

        multi_values = []
        for data in datas:
            for key in field_keys:
                data[key] = self.model.__fields__.get(key).dumps(data[key])

            multi_values.append("({fields_value})".format(
                fields_value=",".join(field_keys_format)
            ).format(**data))

        values_phase = ",".join(multi_values)
        sql = "INSERT INTO {table} ({fields}) VALUES {multi_values}".format(
            table=_table,
            fields=",".join(field_keys),
            multi_values=values_phase
        )
        DEBUG_MSG("DML::insert_many, sql[%s]" % sql)
        KBEngine.executeRawDatabaseCommand(
            sql, Functor(self._insert_cb, cb, datas, _table, sql)
        )

    @db_op
    def find(self, fields, cb, table=None):
        """
        :param fields: select 的字段列表
        :param cb: 回调函数，参数有两个(result_list, error)，表示结果列表和错误信息
        :param table: 表结构名
        :return:
        """
        _table = self._get_table(table)
        filter_phase = self._get_filter_phase()
        if filter_phase:
            sql = "SELECT {fields} FROM {table} WHERE {filter_phase}".format(
                table=_table,
                fields=",".join(fields),
                filter_phase=self._gen_filter_phase(filter_phase)
            )
        else:
            sql = "SELECT {fields} FROM {table}".format(
                fields=",".join(fields),
                table=_table
            )

        if self._orders:
            order_phase = self._get_order_phase()
            sql += order_phase

        if self._limit:
            sql += " limit %s" % self._limit

        DEBUG_MSG("DML::find sql: %s" % sql)
        KBEngine.executeRawDatabaseCommand(
            sql, Functor(self.find_cb, cb, fields, _table, sql)
        )

    def find_cb(self, cb, fields, table, sql, result, rows, insertid, error):
        """
        返回的result的形式如下：
        [[b'69', b'vbnm', b'0', b'0'], [b'70', b'ghjkll', b'0', b'0']]
        1. 如果一列为NULL，则返回在result中的是None
        2. 如果一列是blob类型，返回的任然是bytes类型，例如b'\x80\x03}q\x00.'，
            直接用pickle.loads()
            >>> pickle.loads(b'\x80\x03}q\x00.')
            {}
            >>>
        """
        result_list = []

        if error is not None:
            ERROR_MSG("DML::find_cb, db error, table[%s], error[%s], sql[%s]" %
                      (table, error, sql))

            cb(result_list, error)
            return

        #DEBUG_MSG("find_cb, result: %s" % result)
        for row in result:
            row_result = {}
            for i, field in enumerate(fields):
                v = row[i]
                row_result[field] = self._loads_v(field, v, table, row, sql)

            result_list.append(row_result)

        cb(result_list, error)

    def _loads_v(self, field, v, table, row, sql):
        try:
            return self.model.__fields__.get(field).loads(v)
        except Exception as e:
            ERROR_MSG(
                "DML::_loads_v, loads error: %s, \n "
                "table: %s, row: %s, field: %s, v: %s, sql: %s" %
                (str(e), table, str(row), field, str(v), sql)
            )

    @db_op
    def delete(self, cb=None, table=None, dangerous=False):
        """
        :param dangerous: 为True时用来表示能通过删除所有数据的检查，为False，如果是
                        删除所有，将通不过
        :param table:
        :param cb: 回调函数，回调函数有一个参数（error），
                error为None则无错误，否则就是错误的字符串
        :return:
        """
        filter_phase = self._get_filter_phase()
        if not filter_phase and not dangerous:
            ERROR_MSG("delete operation has no filter phase. It is dangerous")
            return
        _table = self._get_table(table)

        if not filter_phase:
            sql = "DELETE FROM {table}".format(table=_table)
        else:
            sql = "DELETE FROM {table} WHERE {filter_phase}".format(
                table=_table,
                filter_phase=self._gen_filter_phase(filter_phase)
            )
        DEBUG_MSG("DML::delete, sql[%s]" % sql)
        KBEngine.executeRawDatabaseCommand(
            sql, Functor(self._delete_cb, cb, _table, sql)
        )

    def _delete_cb(self, cb, table, sql, result, rows, insertid, error):
        """
        :param cb: 自定义cb，其有一个参数：error，来表明删除过程是否有报错
        :return:
        """
        if error is not None:
            ERROR_MSG("DML::_delete_cb, db error, table[%s], error[%s], sql[%s]"
                      % (table, error, sql))

        # 特别需要强调的地方，如果delete的过滤条件没有过滤到内容，是不会报错的，只是
        # affected rows 会是等于0
        if rows == 0:
            error = db_errors.DbDeleteErrorNotFound()
        else:
            INFO_MSG(
                "DML::_delete_cb, table[%s], sql[%s], affected row[%s]" %
                (table, sql, rows)
            )

        if cb:
            cb(error)

    @db_op
    def execute_custom_sql(self, sql, cb):
        DEBUG_MSG("DML:execute_custom_sql, sql: %s" % sql)
        KBEngine.executeRawDatabaseCommand(sql, cb)

    @db_op
    def update(self, update_data, cb=None, table=None, thread_id=None):
        """
        更新数据库
        :param thread_id:
        :param update_data: 字典，key是更新的字段，value是更新的value
        :param cb: 回调函数，只有一个参数(error)
        :param table:
        :return:
        """
        filter_phase = self._get_filter_phase()
        if not filter_phase:
            WARNING_MSG("update operation has no filter phase. "
                        "It is dangerous. Return")
            return

        if not update_data:
            ERROR_MSG("update operation, update data is empty. Return")
            return

        _table = self._get_table(table)
        update_data_list = []
        for key, value in update_data.items():
            field_type = self.model.__fields__.get(key)
            value = field_type.dumps(value)

            if issubclass(field_type, (STRING, JSON)):
                update_data_list.append("%s='%s'" % (key, value))
            else:
                update_data_list.append("%s=%s" % (key, value))

        sql = "UPDATE {table} SET {update_data_phase} WHERE {filter_phase}".\
            format(
                table=_table,
                update_data_phase=",".join(update_data_list),
                filter_phase=self._gen_filter_phase(filter_phase)
            )
        DEBUG_MSG("DML::update, sql[%s]" % sql)
        if thread_id is None:
            KBEngine.executeRawDatabaseCommand(
                sql, Functor(self._update_cb, cb, _table, sql)
            )
        else:
            KBEngine.executeRawDatabaseCommand(
                sql, Functor(self._update_cb, cb, _table, sql), thread_id
            )

    def _update_cb(self, cb, table, sql, result, rows, insertid, error):
        """
        cb，回调函数，只有一个参数(error)
        """
        if error is not None:
            ERROR_MSG("DML::_update_cb, db error, table[%s], error[%s], sql[%s]"
                      % (table, error, sql))

        # 这里和delete 操作一样，如果过滤条件没有过滤出row去update，则不会报错
        # 然后affected rows 为0。 这里需要给上层调用者加上错误提示上层调用者
        if rows == 0:
            error = db_errors.DbUpdateErrorNotFound()

        if cb:
            cb(error)

    @db_op
    def count(self, cb, table=None):
        _table = self._get_table(table)
        filter_phase = self._get_filter_phase()
        if filter_phase:
            sql = "SELECT COUNT(*) FROM {table} WHERE {filter_phase}".format(
                table=_table,
                filter_phase=self._gen_filter_phase(filter_phase)
            )
        else:
            sql = "SELECT COUNT(*) FROM {table} ".format(
                table=_table
            )

        DEBUG_MSG("DML::count sql: %s" % sql)
        KBEngine.executeRawDatabaseCommand(
            sql, Functor(self._count_cb, cb, _table, sql)
        )

    def _count_cb(self, cb, table, sql, result, rows, insertid, error):
        if error is not None:
            ERROR_MSG("DML::_count_cb, db error, table[%s], error[%s], sql[%s]"
                      % (table, error, sql))
            cb(0, error)
            return

        DEBUG_MSG("count_cb, result: %s" % str(result))
        count = int(result[0][0])
        cb(count, None)

