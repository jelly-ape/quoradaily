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
from quoradaily.story import Story


ST_DEFAULT = 0
ST_PASS = 1  # 通过
ST_DENIAL = -1  # 不通过
__all__ = ['Database', 'ST_DEFAULT', 'ST_PASS', 'ST_DENIAL']
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

    def find_raw(self, status, skip, limit, **conditions):
        """通过日期和状态过滤挑选出 raw 中的数据

        Args:
            status: 状态码. 只能在 ST_DEFAULT, ST_PASS, ST_DENIAL 中
        """
        assert(status in (ST_DEFAULT, ST_PASS, ST_DENIAL))
        conditions['status'] = status
        items = self._db.raw.find(conditions).skip(skip).limit(limit)
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

    def find_story(self, _id):
        """从 ID 号生成 Story 对象

        Args:
            _id: MongoDB 的 _id 号

        Returns:
            返回一个 Story 对象
        """
        story = Story()
        story.title = u"What are some myths people believe about science that are taught by sci-fi movies but aren't true?"
        story.content_html = (u'<p><img src="/static/cover_img.jpg"></p>'
                              u'<p>The reality is that space is so vast and so full of planets and resources that attacking a civilization is literally the most costly wasteful dangerous method imaginable to get whatever resources an alien civilization may want. In real life there is no goddamned reason to fight.</p>'
                              u'<p>If you can find and get to earth via interstellar travel then you can certainly find an uninhabited planet to colonize and whatever mineral deposits or water you might need. Everything a civilization could possibly need is out there waiting to be discovered without any need to fight anyone for it. The universe has, for all practical purposes an unlimited supply of pretty much everything.</p>'
                              u'<p>There are no scarce resources when you can travel anywhere in the galaxy, which has an estimated 400,000,000 stars. In the real universe, interstellar battles are pointless.</p>'
                              u'<p>An alien civilization would have to be unimaginably stupid to build an invasion fleet and attack us. (Which might explain Independence Day and why Jeff Goldblum’s 1995 laptop, with its 8 MB hard drive and no wifi capability was able to bring down an alien fleet.)</p>'
                              u'<p>Edit: To address all the arguments about why an invasion might occur anyway, I want to point out that this carries an immense risk no one seems to have thought of:</p>'
                              u'<p><a href="http://www.baidu.com">Try this site</a>'
                              u'<p>If you’re going to put on the warpaint and go on a galaxy wide conquering rampage it only takes one civilization far more advanced than your own to completely ruin your day. There is always someone better than you. Our galaxy is vast and civilizations might not be what they appear to be.</p>'
                              u'<p>Those harmless looking easy to defeat raccoon people of planet Trashcaneon are unfortunately a bunch of gun nuts -and their gun of choice happens to be a photonic disruption fusion rifle with a personal force shield. Your best weaponry is ineffective and you’re defenseless against their weapons.</p>'
                              u'<p>Your invasion fleet is about as effective as sticks and rocks.</p>'
                              u'<p>The raccoon people and their highly advanced technology don’t take kindly to your interstellar attack and destroy your entire space fleet and crush your home planet and still have time to catch Thursday Night Dumpsterball.</p>')
        story.author_name = u'Craig Weiler'
        story.author_desc = u'Years of training at movie watching'
        story.author_avatar_url = u'https://qph.ec.quoracdn.net/main-thumb-8391952-50-eumqzwejwpfofgfotphchcgfqbybhwhd.jpeg'
        story.cover_url = u'/static/cover_img.jpg'
        story.source_url = u'https://quora.com/What-are-some-myths-people-believe-about-science-that-are-taught-by-sci-fi-movies-but-arent-true/answer/Craig-Weiler'
        return story
