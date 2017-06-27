#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import
from quoradaily.quora_spider import QuoraSpider


def initialize():
    quora = QuoraSpider()
    quora.init_topics()


def crawl():
    quora = QuoraSpider()
    '''
    quora.login('resonx@gmail.com', '411100')
    quora.get_story_list()
    '''
    quora.get_story()



if __name__ == '__main__':
    #initialize()
    crawl()
