#!/usr/bin/env python
# -*- coding: utf-8 -*-
# xiaorixin@2016-10-14T15:04:36

from pyspider.libs.base_handler import *
import copy

# 去爬取的主题名单
TOPICS = ('technology', 'science', 'movies', 'music', 'books', 'health',
          'education', 'food', 'business', 'psychology', 'history', 'cooking',
          'design', 'photography', 'sports', 'economics', 'writing',
          'fashion-and-style', 'philosophy', 'mathematics', 'finance',
          'marketing', 'politics', 'television-series')
# 赞的阈值, 大于该阈值的才被录取
UPVOTE_T = 500


def get_cookies(cookie_str):
    params = cookie_str.split(':')[1]
    params = params.split(';')
    cookies = {}
    for param in params:
        key, value = map(lambda x: x.strip(), param.split('=', 1))
        cookies[key] = value
    return cookies


# 包含登录信息
# 如果失效请在浏览器中登录一次
# 账号: jaquora001@gmail.com
# 密码: woyaofacai
cookie_str = ('Cookie: m-b="9sJADT3vHkD05OQnHXErIw\075\075"; '
              'm-s="fYMfDwTpgebW_CO2WozKUA\075\075"; '
              'm-css_v=325f3decebc009f4; m-login=1; '
              'm-early_v=b76b9b5e84f0f980; m-tz=-480; '
              'm-wf-loaded=q-icons-q_serif; _ga=GA1.2.1595695644.1486820997')
# 通过字符串生成 cookies
cookies = get_cookies(cookie_str)


class Handler(BaseHandler):

    crawl_config = {}

    @every(minutes=5)  # 5m
    def on_start(self):
        """The initial function with all stock symbols
        """
        global TOPICS

        for topic in TOPICS:
            topic = topic.replace(' ', '-')
            url = 'https://www.quora.com/topic/{}'.format(topic)
            self.crawl(url, callback=self.get_topic_list, cookies=cookies,
                       save={'topic': topic})

    def _parse_count(self, count):
        if count.isdigit():
            return int(count)
        elif count.endswith('k'):
            return int(float(count[:-1]) * 1000)
        else:
            raise ValueError('Invalid_Count: {}'.format(count))

    @config(age=60)
    def get_topic_list(self, resp):
        items = resp.etree.xpath('//*[@class="feed_item_inner"]')
        for item in items:
            # 点赞数
            save = copy.copy(resp.save)
            count = item.find_class('count')
            if count:
                count = count[0]
                count = count.text_content()
                count = self._parse_count(count)
                if count < UPVOTE_T:  # 不达标, 不要
                    continue
                else:
                    save['upvote'] = count
            q_text, question_link, user_link, answer_link, count = [None] * 5
            # 问题
            question = item.find_class('question_link')
            if question:
                question = question[0]
                # 问题的链接
                save['question_link'] = question.get('href')
                # 问题的文本
                save['question_text'] = question.text_content()

            # 用户
            user = item.find_class('user')
            if user:
                user = user[0]
                # 用户主页链接
                save['user_link'] = user.get('href')
                # 用户名
                save['user'] = user.text_content()
                # 该用户对于该问题回答的链接
                save['answer_link'] = u'{}/answer/{}'.format(
                    save['question_link'],
                    save['user_link'].rsplit(u'/', 1)[1],
                )

            # 用户的描述信息
            user_desc = item.find_class('IdentityNameCredential')
            if user_desc:
                user_desc = user_desc[0]
                save['user_description'] = user_desc.text_content()

            # 用户的头像
            user_img = item.find_class('profile_photo_img')
            if user_img:
                user_img = user_img[0]
                save['user_img'] = user_img.get('src')

            # 回答的摘要
            answer_abstract = item.find_class('truncated_q_text')
            if answer_abstract:
                save['answer'] = answer_abstract[0].text_content()

            yield save
            # 反正有编辑会加入到审核过程中, 不需要去爬取对应资源, 直接让编辑去
            # quora 网站上看即可
#            if 'answer_link' in save:
#                answer_url = 'https://www.quora.com/' + save['answer_link']
#                self.crawl(answer_url, save=save, callback=self.get_answer)

    @config(age=24 * 60 * 60)  # 一天
    def get_answer(self, resp):
        answer = resp.etree.xpath('//*[contains(@class, "AnswerPageAnswer ")]')
        item = answer[0].find_class('rendered_qtext')[0]
        children = item.iterchildren()
        content = []
        for child in children:
            if child.tag == 'p':
                # TODO: 没处理链接的情况
                p = child.text_content().strip()
                if p:
                    content.append(p)
            elif child.tag == 'div' \
                    and child.get('class') == 'qtext_image_wrapper':  # 图片
                imgs = child.iterchildren('img')
                for img in imgs:
                    src = img.get('master_src').strip()
                    if src:
                        content.append(src)
            # TODO: 没处理视频/音频和其他可能的媒体的情况

        if content:
            save = copy.copy(resp.save)
            save['answer'] = content
            return save
