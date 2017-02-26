#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据库的业务封装

spider -> raw -> online
其中:
    spider: 只读
    raw: 可以编辑, 编辑完成后保存, 点击上线复制到 online
    online: 只读, 可以被 raw 覆盖
"""
import copy
import time
import pymongo
import logging
from datetime import datetime
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError
try:
    import ujson as json
except ImportError:
    import json
# 己方库
from quoradaily.libs import singleton, get_config


ST_DEFAULT = 0
ST_PASS = 1  # 通过
ST_DENIAL = -1  # 不通过
__all__ = ['Database', 'ST_DEFAULT', 'ST_PASS', 'ST_DENAIL']
logger = logging.getLogger(__name__)


@singleton
class Database(object):
    """需要用到的数据库功能的封装
    """

    def __init__(self):
        """初始化并建立索引. 读取配置文件
        """
        config = get_config()
        mongo_URI = config.get('db', 'mongo_URI')
        # 爬虫的数据库
        spider_db_name = config.get('db', 'spider_db')
        # 没添加到线上数据库的数据
        db_name = config.get('db', 'db_name')

        self._client = pymongo.MongoClient(mongo_URI)
        self._spider_db = self._client[spider_db_name]
        self._db = self._client[db_name]

        self._init_index()
        object.__init__(self)

    def _init_index(self):
        """建立索引
        """
        self._db.raw.ensure_index([('answer_link', 1)], unique=True)

    def __del__(self):
        self._client.close()

    def trans_spider(self, beg_date, end_date):
        """获取爬虫爬到的原始数据, 转移到 `raw` collection 中

        Args:
            beg_date: 起始日期, 闭区间 (datetime)
            end_date: 结束时间, 开区间 (datetime)
        """
        assert(type(beg_date) is datetime)
        assert(type(end_date) is datetime)
        assert(beg_date < end_date)
        beg_ts = time.mktime(beg_date.timetuple())
        end_ts = time.mktime(end_date.timetuple())

        conditions = {'updatetime': {'$gte': beg_ts, '$lt': end_ts}}
        items = self._spider_db.quora_topic.find(conditions)
        for item in items:
            result = json.loads(item['result'])
            q_link = result['question_link'].strip()
            a_link = result['answer_link'].strip()
            q_title = result['question_text'].strip()
            a_abstract = str(result['answer']).strip()
            topic = result['topic'].strip().strip()
            if q_link and a_link and q_title and a_abstract and topic:
                try:
                    self._db.raw.insert({
                        'question_link': q_link,
                        'answer_link': a_link,
                        'question_title': q_title,
                        'answer_abstract': a_abstract,
                        'topic': topic,
                        'date': datetime.today(),
                        'status': ST_DEFAULT,
                    })
                    logger.info(u'item_inserted. answer_link={}'.format(
                        a_link
                    ))
                except DuplicateKeyError:
                    logger.warn(u'item_exists. answer_link={}'.format(
                        a_link
                    ))

    def find_raw(self, beg_date, end_date, status):
        """通过日期和状态过滤挑选出 raw 中的数据

        Args:
            beg_date: 起始日期, 闭区间 (datetime)
            end_date: 结束时间, 开区间 (datetime)
            status: 状态码. 只能在 ST_DEFAULT, ST_PASS, ST_DENIAL 中
        """
        assert(type(beg_date) is datetime)
        assert(type(end_date) is datetime)
        assert(beg_date < end_date)
        assert(status in (ST_DEFAULT, ST_PASS, ST_DENIAL))
        conditions = {'date': {'$gte': beg_date, '$lt': end_date},
                      'status': status}
        items = self._db.raw.find(conditions)
        for item in items:
            yield item

    def update_raw(self, _id, status):
        """更新 raw 中数据的状态

        Args:
            _id: 被更新对象的 ID, 支持字符串和 ObjectID
            status: 需要更新的目标状态码.
                    只能在 ST_DEFAULT, ST_PASS, ST_DENIAL 中
        """
        assert(status in (ST_DEFAULT, ST_PASS, ST_DENIAL))
        if type(_id) is not ObjectId:
            _id = ObjectId(_id)
        ret = self._db.raw.update({'_id': _id}, {'$set': {'status': status}})
        logger.info('update_raw id={}, status={}'.format(str(_id), status))
        return ret
