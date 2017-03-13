#!/usr/bin/env python
# -*- coding: utf-8 -*-
try:
    import ujson as json
except ImportError:
    import json


class Story(object):
    """一个问题对应的一个回答, 称之为 Story. 应该包含如下信息:

    1. 标题 title
    2. 内容 content_html
    3. 封面图片 cover_url
    4. 作者信息 author
        4.1. 作者名称 name
        4.2. 作者描述 desc
        4.3. 作者头像 avatar_url
    5. 原文链接: source_url
    """

    def __init__(self):
        self.title = None
        self.content_html = None
        self.cover_url = None
        self.author_name = None
        self.author_desc = None
        self.author_avatar_url = None
        self.source_url = None

    def to_dict(self):
        """将对象打包成 dict 对象
        """
        dict_obj = {
            'title': self.title,
            'content_html': self.content_html,
            'author': {
                'name': self.author_name,
                'desc': self.author_desc,
                'avatar_url': self.author.avatar_url,
            },
            'cover_url': self.cover_url,
            'source_url': self.source_url,
        }
        return dict_obj

    def to_json(self):
        """将对象打包成 json
        """
        return json.dumps(self.to_dict())

    @classmethod
    def from_id(cls, _id):
        """从数据库中获取数据, 生成对象
        """
        pass
