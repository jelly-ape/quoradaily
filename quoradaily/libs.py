#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
常用的函数们
"""
from __future__ import absolute_import
import os
import heapq
import logging
import ConfigParser


# 存放 ConfigParser 对象
__config = None
# 配置文件目录相对当前文件的路径
RELATIVE_PATH = '../conf'


def get_config(config_file='config.cfg', **kwargs):
    """获取 config 对象

    Args:
        config_file: config 文件名称, 路径默认为 ../conf

    Returns:
        返回 ConfigParser 对象
    """
    global __config, RELATIVE_PATH

    if __config is None:
        config = ConfigParser.ConfigParser()
        # ROOT_PATH/conf/config.cfg
        config_file = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            RELATIVE_PATH,
            config_file
        )
        config.read(config_file)
        __config = config

    return __config


def mkdir(path):
    """实现 `mkdir -p` 的效果

    Args:
        path: 需要建立的路径
    """
    if path == '/':
        return

    head, tail = os.path.split(path)
    if head:
        mkdir(head)

    try:
        os.mkdir(path)
    except OSError:
        pass
    except Exception as e:
        raise e


def init_logging(log_path=None):
    """初始化日志
    """
    LOG_FORMAT = ('%(levelname)s %(asctime)s %(filename)s:'
                  '%(lineno)d\t%(message)s')
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    LOG_LEVEL = logging.INFO
    if log_path is None:
        logging.basicConfig(
            level=LOG_LEVEL,
            format=LOG_FORMAT,
            datefmt=TIME_FORMAT,
        )
    else:
        mkdir(log_path)
        logging.basicConfig(
            level=LOG_LEVEL,
            format=LOG_FORMAT,
            datefmt=TIME_FORMAT,
            filename=log_path,
        )


def files_of_dir(dir_path):
    """获取目标目录下所有的文件
    """
    if not os.path.isdir(dir_path):
        raise ValueError('{} is not a dir'.format(dir_path))

    for root, dirs, files in os.walk(dir_path):
        for f in files:
            yield os.path.abspath(root), f


def _U(i, auto=False):
    """将字符串转换为 unicode. 在不开启自动检测功能时仅考虑 UTF-8 情况.

    Args:
        i: 输入字符串或其他
        auto: 是否自动匹配输入字符串的编码格式, 默认为 False.

    Returns:
        如果输入是 str 对象则参与变换, 否则直接返回
    """
    if type(i) is not str:  # 返回输入的
        return i

    o = None
    try:
        o = i.decode('utf-8')
    except UnicodeDecodeError as e:
        if auto:
            encoding = detect(i)['encoding']
            o = i.decode(encoding)
        else:
            raise e
    return o


def singleton(cls, *args, **kwargs):
    """单例模式修饰器

    Args:
        cls: 类
    """
    instances = {}

    def _singleton(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return _singleton


class Heap(object):
    """自带的 heapq 不好用, 对其进行了简单的封装
    小头堆, 返回的是最大的元素
    """

    def __init__(self, n):
        """初始化

        Args:
            n: 堆的最大容量
        """
        self.n = n
        self.heap = []
        self.words = set()

    def add(self, val):
        """添加一个元素

        Args:
            val: 元素, 格式为 (label, weight).
        """
        if len(self.heap) < self.n:
            heapq.heappush(self.heap, val)
            word = val[0]
            self.words.add(word)
        elif self.heap[0][1] < val[1]:
            word, _ = heapq.heapreplace(self.heap, val)
            self.words.discard(word)
            word = val[0]
            self.words.add(word)

    def sort(self, reverse=True):
        """将结果排序返回

        Args:
            reverse: 是否翻转 (是 -> 大的优先)
        """
        self.heap.sort(key=lambda x: x[1], reverse=reverse)
        return self.heap

    def __contains__(self, word):
        return word in self.words
