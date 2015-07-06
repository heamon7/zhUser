# -*- coding: utf-8 -*-
import scrapy


class UserpostSpider(scrapy.Spider):
    name = "userPost"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )

    def parse(self, response):
        pass


# 这里暂时空白，主要是专栏页下不规则，不能区分是否属于用户 如 http://www.zhihu.com/people/fenng/posts