#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import pymongo


class Database(object):

    def __init__(self, host='localhost', port=27017, db_name='quora',
                 *args, **kwargs):
        self._client = pymongo.MongoClient(host, port)
        self._db = self._client[db_name]
        self._db.spider_topics.ensure_index(
            [('url', 1)], unique=True
        )
        self._db.spider_tasks.ensure_index(
            [('answer_link', 1)], unique=True
        )

    def insert_topic(self, topic_name, url, **other_params):
        """插入一个 topic. 如果已经出现的 topic 会自动忽略.

        Args:
            topic_name: 主题名称
            url: topic 的 URL
            other_params: 其他参数
        """
        insert_item = other_params
        insert_item['topic_name'] = topic_name
        insert_item['url'] = url
        try:
            self._db.spider_topics.insert(insert_item)
        except pymongo.errors.DuplicateKeyError:  # 已经存在, 忽略
            pass

    def find_topic(self):
        """获取所有的 topic
        """
        return self._db.spider_topics.find()

    def insert_task(self, answer_link, upvote, question, **other_params):
        """插入一个 task, 如果已经存在就更新.

        Args:
            answer_link: 回答的链接
            upvote: 点赞数
            question: 问题
        """
        insert_item = other_params
        insert_item.update({
            'answer_link': answer_link,
            'upvote': upvote,
            'question': question,
        })
        try:
            return self._db.spider_tasks.insert(insert_item)
        except pymongo.errors.DuplicateKeyError:  # 已经存在, 忽略
            return self._db.spider_tasks.update(
                {'answer_link': answer_link},
                {'$set': insert_item},
            )

    def find_task(self, conditions):
        """获取任务
        """
        return self._db.spider_tasks.find(conditions)

    def save_task(self, task):
        """保存任务
        """
        return self._db.spider_tasks.save(task)
