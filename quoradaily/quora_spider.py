#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
import time
import random
import requests
import lxml.html as html
from bs4 import BeautifulSoup as bs
from selenium.webdriver import PhantomJS
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from quoradaily.libs.db import Database
from quoradaily.libs.log import get_logger
"""
参考: http://www.jianshu.com/p/9d408e21dc3a
https://www.quora.com/sitemap
"""

logger = get_logger('quora_spider')


class QuoraSpider(object):

    user_agents = (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    )
    scroll_script = '''window.scrollTo(0,document.body.scrollHeight);'''

    def __init__(self, waiting_sec=10):
        self.url = 'https://www.quora.com'
        self.waiting_sec = waiting_sec
        self.db = Database()
        self.__inited_phantomjs = False

    def __init_phantomjs(self):
        if self.__inited_phantomjs:
            return

        dcap = dict(DesiredCapabilities.PHANTOMJS)
        dcap["phantomjs.page.settings.userAgent"] = (
            random.choice(self.__class__.user_agents)
        )
        self.driver = PhantomJS(
            desired_capabilities=dcap,
        )
        # 设置页面大小
        self.driver.set_window_size(1920, 1080)
        self.__inited_phantomjs = True

    def __del__(self):
        if 'driver' in self.__dict__:
            self.driver.close()

    def _GET(self, url, params={}, retry=3, sleep_sec=1):
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;'
                      'q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, sdch, br',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Host': 'www.quora.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/58.0.3029.110 Safari/537.36',
        }
        for i in xrange(retry):
            try:
                resp = requests.get(url, params=params, headers=headers)
                assert resp.status_code == requests.codes.ok
                return resp
            except Exception:
                exc_info = logger.exc_info()
                logger.warn('request_retry_sleep', url=url, params=params,
                            retry=i + 1, sleep_sec=sleep_sec, **exc_info)
                time.sleep(sleep_sec)
        else:
            logger.error('request_retry_max', url=url, params=params)
            raise requests.exceptions.ConnectionError(
                u'retry_max, retry={}, url={}'.format(retry, url)
            )

    def init_topics(self):
        """从 sitemap 中找到最常多人 follow 的若干主题
        """
        url = 'https://www.quora.com/sitemap'
        resp = self._GET(url)
        root = html.fromstring(resp.content)
        items = root.xpath('//a[contains(@class, "TopicNameLink")]')
        for item in items:
            try:
                url = item.get('href')
                name = item.text_content()
                self.db.insert_topic(name, url)
                logger.info('insert_topic_success', name=name, url=url)
            except:
                exc_info = logger.exc_info()
                logger.warn('insert_topic_error', **exc_info)

    def login(self, email, password):
        """登录, 不登录将没法获取 cookies
        """
        self.__init_phantomjs()
        self.driver.get(self.url)
        email_input = self.driver.find_element_by_xpath(
            '//form[@class="inline_login_form"]//input[@name="email"]'
        )
        email_input.send_keys(email)
        password_input = self.driver.find_element_by_xpath(
            '//form[@class="inline_login_form"]//input[@name="password"]'
        )
        password_input.send_keys(password)
        login_btn = self.driver.find_element_by_xpath(
            '//form[@class="inline_login_form"]//input[@type="submit"]'
        )
        login_btn.click()
        logger.info('login', email=email)
        if self.waiting_sec > 0:
            time.sleep(self.waiting_sec)
            logger.info('login_sleep_done', sleep_sec=self.waiting_sec)

    def get_story_list(self, page_count=10):
        """获取所有关注主题的 story

        Args:
            page_count: 翻页页数
        """

        def _parse_count(count):
            if count.isdigit():
                return int(count)
            elif count.endswith('k'):
                return int(float(count[:-1]) * 1000)
            else:
                return -1

        for topic in self.db.find_topic():
            name = topic['topic_name']
            url = topic['url']
            logger.info('start_to_get_story_list', topic=name,
                        total_page=page_count, url=url)

            # 加载页面
            self.driver.get('https://www.quora.com' + url)
            for i in xrange(page_count):
                self.driver.execute_script(self.__class__.scroll_script)
                logger.info('get_story_list', topic=name, page=i + 1,
                            total_page=page_count)
                if self.waiting_sec > 0:
                    time.sleep(self.waiting_sec)

            # 获取数据
            items = self.driver.find_elements_by_xpath(
                '//*[@class="feed_item_inner"]'
            )
            for item in items:
                # 点赞数
                try:
                    count = item.find_element_by_class_name('count').text
                    count = _parse_count(count)
                    assert count > 0

                    # 问题
                    question = item.find_element_by_class_name(
                        'question_link'
                    )
                    q_link = question.get_attribute('href')
                    q_text = question.find_element_by_class_name(
                        'rendered_qtext'
                    ).text

                    # 用户
                    user = item.find_element_by_class_name('user')
                    u_link = user.get_attribute('href')

                    # 答案链接
                    a_link = u'{}/answer/{}'.format(
                        q_link,
                        u_link.rsplit(u'/', 1)[1],
                    )
                    self.db.insert_task(a_link, count, q_text)
                    logger.info('insert_task_success', question=q_text,
                                upvote=count)
                except Exception as e:
                    exc_info = logger.exc_info()
                    logger.warn(**exc_info)
                    continue

            logger.info('get_story_list_done', topic=name)

    def get_story(self):

        def _hump2underline(line):
            """驼峰转下划线
            """
            ret = []
            for i, c in enumerate(line):
                if c.isupper() and i != 0:
                    ret.append('_')
                ret.append(c.lower())
            return ''.join(ret)

        conditions = {
            'content_html': {'$exists': False},
            'upvote': {'$gt': 500},
        }
        tasks = self.db.find_task(conditions)
        for task in tasks:
            try:
                url = task['answer_link']
                resp = self._GET(url)
                soup = bs(resp.content, 'html.parser')

                content = soup.find('div', class_='ExpandedAnswer')
                content = content.find('span', class_='rendered_qtext')
                task['content_html'] = unicode(content)

                author = soup.find('div', class_='AboutAuthorSection')
                task['author'] = {}
                author_avatar = author.find(
                    'img', class_='profile_photo_img'
                ).get('src')
                task['author']['avatar_url'] = author_avatar

                author_items = author.find_all('div', class_='AboutListItem')
                for item in author_items:
                    try:
                        item_name = filter(
                            lambda x: x not in ['CredentialListItem',
                                                'AboutListItem'],
                            item.get('class')
                        )
                        item_name = _hump2underline(item_name[0][:-8])
                        main = item.find('span', class_='main_text').get_text()
                        task['author'][item_name] = {}
                        task['author'][item_name]['main'] = main
                        detail = item.find('span',
                                           class_='detail_text').get_text()
                        task['author'][item_name]['detail'] = detail
                    except:
                        continue

                self.db.save_task(task)
                logger.info('get_story_success', question=task['question'],
                            upvote=task['upvote'])
            except:
                exc_info = logger.exc_info()
                logger.warn(
                    'get_story_error',
                    question=item.get('question', ''),
                    upvote=item.get('upvote', ''),
                    **exc_info
                )
