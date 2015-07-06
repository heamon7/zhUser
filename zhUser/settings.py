# -*- coding: utf-8 -*-

# Scrapy settings for zhUser project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'zhUser'

SPIDER_MODULES = ['zhUser.spiders']
NEWSPIDER_MODULE = 'zhUser.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'zhUser (+http://www.yourdomain.com)'


DOWNLOAD_TIMEOUT = 700

LOG_LEVEL = 'INFO'

DEFAULT_REQUEST_HEADERS = {
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
           'Accept-Encoding': 'gzip, deflate, sdch',
           'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,zh-TW;q=0.2',
           'Connection': 'keep-alive',
           'Host': 'www.zhihu.com',
           'Referer': 'http://www.zhihu.com/',

}

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36'

EXTENSIONS = {
    # 'scrapy.contrib.feedexport.FeedExporter': None,
    'scrapy.extensions.feedexport.FeedExporter': None

}

ITEM_PIPELINES = {
    'zhUser.pipelines.UserInfoPipeline': 300,
    'zhUser.pipelines.UserFolloweePipeline': 350,
    'zhUser.pipelines.UserFollowerPipeline': 400,
    'zhUser.pipelines.UserColumnPipeline': 450,
    'zhUser.pipelines.UserTopicPipeline': 500,
    'zhUser.pipelines.UserAskPipeline': 550,
    'zhUser.pipelines.UserAnswerPipeline': 600,
    'zhUser.pipelines.UserCollectionPipeline': 650,
    'zhUser.pipelines.UserActivityPipeline': 700,


}
SPIDER_MIDDDLEWARES = {
    'scrapy.contrib.spidermiddleware.httperror.HttpErrorMiddleware':300,
}

DUPEFILTER_CLASS = 'zhUser.custom_filters.SeenURLFilter'



INFO_UPDATE_PERIOD = '432000' #最快5天更新一次
COLUMN_UPDATE_PERIOD = '432000' #最快5天更新一次
COLLECTION_UPDATE_PERIOD = '432000' #最快5天更新一次


