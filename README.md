# kbengine-orm
mysql orm for [kbengine](https://github.com/kbengine/kbengine)

对kbegnine中的mysql操作，简单做了一下ORM封装

# feature

* 支持查找 find
* 支持插入 insert
* 支持批量插入 insert many
* 支持删除 delete
* 支持更新 update
* 支持存在更新，不存在插入功能 `dup_key_update`
* 支持的过滤等操作指令有:
    * gt, gte：大于，大于等于
    * lt, lte：小于，小于等于
    * eq：相等
    * neq：不相等
    * order_by：排序
    * limit：返回行数限制
    * in：in操作

# Quick start

来看个例子
```
from dbs.db_base import BaseModel
from dbs.columns import INT
import time

class TestModel(BaseModel):

    __table__ = "table_name"
    __fields__ = {
        "phone": INT,
        "code": INT,
        "time": INT,
        "enable": INT,
    }

    def __init__(self):
        super(TestModel, self).__init__()
    
    def find_phone(self):
        # 查找过滤，可以链式调用
        self.dml.eq("phone", "1111").lt("time", 1111).limit(10)
        self.dml.find(["phone", "code"], self._find_phone_cb)

    def _find_phone_cb(result_list, error):
        """
        result_list: 是一个list，元素是字典，每个字典是要查询的字段的key value值
        这个例子中就是 [{"phone": "111", "code": 1}, {"phone": "222", "code": 2}]
        """
        pass

    def upsert_sms_code(self, phone, code, time, cb):
        data = {
            "phone": phone,
            "code": code,
            "time": time,
            "enable": 1
        }
        # 如果存在就更新，不存在就插入，使用了mysql的dup_key_update功能
        self.dml.insert(data, cb, dup_key_update=True, update_data=data)

    def set_v_code_used(self, phone):
        data = {
            "enable": 2
        }
        self.dml.eq("phone", phone).update(data)

    def delete_row(self, phone):
        self.dml.eq("phone", phone)
        self.dml.delete()

```

使用步骤：

* 创建一个model类，继承`BaseModel`类
* 写表的name, `__table__`
* 写表的字段以及类型，`__fields__`
* 这样类可以使用 `self.dml`实例，这是一个DML实例，可以通过此实例进行Mysql的各种相关的操作，可以看看此类的源码了解更多
* 另外，如果有比较复杂的sql语句，没有接口可以满足，可以使用`execute_custom_sql`函数

在kbengine中使用，就将`dbs`目录拷贝到`server_common`下面即可

# 完善与改进

* 现在这个功能封装很简单，和市面上的开源的ORM框架没得比，我这只提供简单的mysql操作，对于kbengine这样面向对象的写代码方式来说大多数是够用了
* 现在kbengine支持多数据库，此库还不支持选择数据库，还是使用`default`数据库



