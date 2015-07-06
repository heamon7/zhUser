# -*- coding: utf-8 -*-
import scrapy


class UserlogSpider(scrapy.Spider):
    name = "userLog"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )

    def parse(self, response):
        pass


# 这里暂时空白，编辑日志的意义貌似不太大