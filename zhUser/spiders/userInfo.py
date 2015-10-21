# -*- coding: utf-8 -*-
import scrapy

from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request,FormRequest
from scrapy.shell import inspect_response

import datetime
import re
import redis
import requests
import logging

from zhUser import settings
from zhUser.items import UserInfoItem
from pymongo import MongoClient


class UserinfoSpider(scrapy.Spider):
    name = "userInfo"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )
    baseUrl = "http://www.zhihu.com/people/%s/about"
    handle_httpstatus_list = [429,502,504]
    quesIndex =0
    userDataLinkIdDict = {}


    def __init__(self,stats,spider_type='Master',spider_number=0,partition=1,**kwargs):
        self.stats = stats

        # redis2 以list的形式存储有所有问题的id和问题的info，包括answerCount
        self.redis_client = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=settings.USER_LINK_ID_REDIS_DB_NUMBER)
        self.client = MongoClient(settings.MONGO_URL)
        self.db = self.client['zhihu']
        self.col_log = self.db['log']

        crawler_log = {'project':settings.BOT_NAME,
                       'spider':self.name,
                       'spider_type':spider_type,
                       'spider_number':spider_number,
                       'partition':partition,
                       'type':'start',
                       'updated_at':datetime.datetime.now()}

        self.col_log.insert_one(crawler_log)
        try:
            self.spider_type = str(spider_type)
            self.spider_number = int(spider_number)
            self.partition = int(partition)
            # self.email= settings.EMAIL_LIST[self.spider_number]
            # self.password=settings.PASSWORD_LIST[self.spider_number]

        except:
            self.spider_type = 'Master'
            self.spider_number = 0
            self.partition = 1
            # self.email= settings.EMAIL_LIST[self.spider_number]
            # self.password=settings.PASSWORD_LIST[self.spider_number]

    @classmethod
    def from_crawler(cls, crawler,spider_type='Master',spider_number=0,partition=1,**kwargs):
        return cls(crawler.stats,spider_type=spider_type,spider_number=spider_number,partition=partition)
    def start_requests(self):


        #这里得到的结果应该是一一对应的吧


        self.userLinkIdList = list(self.redis_client.smembers('user_link_id'))
        totalLength = len(self.userLinkIdList)

        if self.spider_type=='Master':
            logging.warning('Master spider_type is '+self.spider_type)
            if self.partition!=1:
                logging.warning('Master non 1 partition is '+str(self.partition))
                self.userLinkIdList = self.userLinkIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]

                for index in range(1,self.partition):
                    payload ={
                        'project':settings.BOT_NAME
                        ,'spider':self.name
                        ,'spider_type':'Slave'
                        ,'spider_number':index
                        ,'partition':self.partition
                        ,'setting':'JOBDIR=/tmp/scrapy/'+self.name+str(index)
                    }
                    logging.warning('Begin to request'+str(index))
                    response = requests.post('http://'+settings.SCRAPYD_HOST_LIST[index]+':'+settings.SCRAPYD_PORT_LIST[index]+'/schedule.json',data=payload)
                    logging.warning('Response: '+str(index)+' '+str(response))
            else:
                logging.warning('Master number is '+str(self.spider_number) + ' partition is '+str(self.partition))

        elif self.spider_type =='Slave':
            logging.warning('Slave spider_type is '+self.spider_type)
            logging.warning('Slave number is '+str(self.spider_number) + ' partition is '+str(self.partition))
            if (self.partition-self.spider_number)!=1:
                self.userLinkIdList = self.userLinkIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
            else:
                self.userLinkIdList = self.userLinkIdList[self.spider_number*totalLength/self.partition:]
        else:
            logging.warning('spider_type is:'+str(self.spider_type)+'with type of '+str(type(self.spider_type)))
        logging.warning('start_requests ing ......')
        logging.warning('totalCount to request is :'+str(len(self.userLinkIdList)))
        yield Request(url ='http://www.zhihu.com',
                      cookies=settings.COOKIES_LIST[self.spider_number],
                      callback =self.after_login)

    #     yield Request("http://www.zhihu.com",callback = self.post_login)
    #
    #
    # def post_login(self,response):
    #     xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]
    #     yield FormRequest.from_response(response,
    #                                       formdata={
    #                                           '_xsrf':xsrfValue,
    #                                           'email':self.email,
    #                                           'password':self.password,
    #                                           'rememberme': 'y'
    #                                       },
    #                                       dont_filter = True,
    #                                       callback = self.after_login,
    #                                       )

    def after_login(self,response):
        try:
            loginUserLink = response.xpath('//div[@id="zh-top-inner"]/div[@class="top-nav-profile"]/a/@href').extract()[0]
            logging.warning('Successfully login with %s  ',str(loginUserLink))
        except:
            logging.error('Login failed! %s  ',self.email)
        #inspect_response(response,self)
        #self.urls = ['http://www.zhihu.com/question/28626263','http://www.zhihu.com/question/22921426','http://www.zhihu.com/question/20123112']
        for index, userLinkId in enumerate(self.userLinkIdList):
            # logging.warning('userLinkId:  %s',userLinkId)

            yield Request(url = self.baseUrl %str(userLinkId)
                          ,meta={'userLinkId':userLinkId}
                          ,callback=self.parsePage)


    def parsePage(self,response):
        if response.status != 200:
#            print "ParsePage HTTPStatusCode: %s Retrying !" %str(response.status)
            yield  Request(url = response.request.url
                           ,meta={'userLinkId':response.meta['userLinkId']}
                           ,callback=self.parsePage)

        else:

            item =  UserInfoItem()

            item['spiderName'] = self.name

            item['userLinkId'] = response.meta['userLinkId']


            try:
                item['userWeiboId'] = response.xpath('//div[@class="zm-profile-header"]//a[@class="zm-profile-header-user-weibo"]/@href').re(r'http://weibo.com/u/(\d+)')[0]  #right
            except:
                item['userWeiboId']=''

            item['userName'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"title-section")]/a[@class="name"]/text()').extract()[0]

            try:
                item['userBio'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"title-section")]/span[@class="bio"]/text()').extract()[0]
            except:
                item['userBio'] = ''
            item['userImgLink'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-avatar-container")]/img/@src').extract()[0]

            selProfile =response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]')

            try:
                selLocation = selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="location"]//span[contains(@class,"location")]/a')
                item['userLocationTopicLinkId'] = selLocation.xpath('@href').re('/topic/(\d+)')[0]
                item['userLocationTopicDataToken'] = selLocation.xpath('@data-token').extract()[0]
                item['userLocationTopicId'] = selLocation.xpath('@data-topicid').extract()[0]
                item['userLocationText'] = selLocation.xpath('text()').extract()[0]
            except:
                try:
                    item['userLocationTopicLinkId'] = ''
                    item['userLocationTopicDataToken'] = ''
                    item['userLocationTopicId'] =''
                    item['userLocationText'] = selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="location"]//span[contains(@class,"location")]/text()').extract()[0]
                # item['userLocationTopicLink'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="location"]//span[contains(@class,"location")]/a/@href').extract()[0]
                # item['userLocationTopicDataToken'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="location"]//span[contains(@class,"location")]/a/@data-token').extract()[0]
                # item['userLocationTopicId'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="location"]//span[contains(@class,"location")]/a/@data-topicid').extract()[0]
                # item['userLocationText'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="location"]//span[contains(@class,"location")]/a/text()').extract()[0]


                except:
                    item['userLocationTopicLinkId'] = ''
                    item['userLocationTopicDataToken'] = ''
                    item['userLocationTopicId'] =''
                    item['userLocationText'] = ''

            try:
                selBusiness = selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="location"]//span[contains(@class,"business")]/a')
                item['userBusinessTopicLinkId'] = selBusiness.xpath('@href').re(r'/topic/(\d+)')[0]
                item['userBusinessTopicDataToken'] = selBusiness.xpath('@data-token').extract()[0]
                item['userBusinessTopicId'] = selBusiness.xpath('@data-topicid').extract()[0]
                item['userBusinessText'] = selBusiness.xpath('text()').extract()[0]
            except:
                try:
                    item['userBusinessText'] = selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="location"]//span[contains(@class,"business")]/text()').extract()[0]
                    item['userBusinessTopicLinkId'] = ''
                    item['userBusinessTopicDataToken'] = ''
                    item['userBusinessTopicId'] = ''
                # item['userBusinessTopicLink'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="location"]//span[contains(@class,"business")]/a/@href').extract()[0]
                # item['userBusinessTopicDataToken'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="location"]//span[contains(@class,"business")]/a/@data-token').extract()[0]
                # item['userBusinessTopicId'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="location"]//span[contains(@class,"business")]/a/@data-topicid').extract()[0]
                # item['userBusinessText'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="location"]//span[contains(@class,"business")]/a/text()').extract()[0]
                except:
                    item['userBusinessTopicLinkId'] = ''
                    item['userBusinessTopicDataToken'] = ''
                    item['userBusinessTopicId'] = ''
                    item['userBusinessText'] = ''

            try:
                item['userGender'] = selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="location"]//span[contains(@class,"gender")]/i/@class').re(r'icon icon-profile-(\w*)')[0]
            except:
                item['userGender'] = ''
            try:
                selEmployment = selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="employment"]//span[contains(@class,"employment")]/a')
                item['userEmploymentTopicLinkId'] = selEmployment.xpath('@href').re(r'/topic/(\d+)')[0]
                item['userEmploymentTopicDataToken'] = selEmployment.xpath('@data-token').extract()[0]
                item['userEmploymentTopicId'] = selEmployment.xpath('@data-topicid').extract()[0]
                item['userEmploymentText'] = selEmployment.xpath('text()').extract()[0]
                # item['userEmploymentTopicLink'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="employment"]//span[contains(@class,"employment")]/a/@href').extract()[0]
                # item['userEmploymentTopicDataToken'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="employment"]//span[contains(@class,"employment")]/a/@data-token').extract()[0]
                # item['userEmploymentTopicId'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="employment"]//span[contains(@class,"employment")]/a/@data-topicid').extract()[0]
                # item['userEmploymentText'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="employment"]//span[contains(@class,"employment")]/a/text()').extract()[0]
            except:
                try:
                    item['userEmploymentText'] = selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="employment"]//span[contains(@class,"employment")]/text()').extract()[0]
                    item['userEmploymentTopicLinkId'] = ''
                    item['userEmploymentTopicDataToken'] = ''
                    item['userEmploymentTopicId'] = ''

                except:
                    item['userEmploymentTopicLinkId'] = ''
                    item['userEmploymentTopicDataToken'] = ''
                    item['userEmploymentTopicId'] = ''
                    item['userEmploymentText'] = ''

            try:
                selPosition = selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="employment"]//span[contains(@class,"position")]/a')
                item['userPositionTopicLinkId'] = selPosition.xpath('@href').re(r'/topic/(\d+)')[0]
                item['userPositionTopicDataToken'] = selPosition.xpath('@data-token').extract()[0]
                item['userPositionTopicId'] = selPosition.xpath('@data-topicid').extract()[0]
                item['userPositionText'] = selPosition.xpath('text()').extract()[0]
                # item['userPositionTopicLink'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="employment"]//span[contains(@class,"position")]/a/@href').extract()[0]
                # item['userPositionTopicDataToken'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="employment"]//span[contains(@class,"position")]/a/@data-token').extract()[0]
                # item['userPositionTopicId'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="employment"]//span[contains(@class,"position")]/a/@data-topicid').extract()[0]
                # item['userPositionText'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="employment"]//span[contains(@class,"position")]/a/text()').extract()[0]
            except:
                try:
                    item['userPositionText'] = selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="employment"]//span[contains(@class,"position")]/text()').extract()[0]
                    item['userPositionTopicLinkId'] = ''
                    item['userPositionTopicDataToken'] = ''
                    item['userPositionTopicId'] =''


                except:
                    item['userPositionTopicLinkId'] = ''
                    item['userPositionTopicDataToken'] = ''
                    item['userPositionTopicId'] =''
                    item['userPositionText'] = ''

            try:
                selEducation = selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="education"]//span[contains(@class,"education")]/a')
                item['userEducationTopicLinkId'] = selEducation.xpath('@href').re(r'/topic/(\d+)')[0]
                item['userEducationTopicDataToken'] = selEducation.xpath('@data-token').extract()[0]
                item['userEducationTopicId'] = selEducation.xpath('@data-topicid').extract()[0]
                item['userEducationText'] = selEducation.xpath('text()').extract()[0]
            except:
                try:
                    item['userEducationText'] = selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="education"]//span[contains(@class,"education")]/text()').extract()[0]
                    item['userEducationTopicLinkId'] = ''
                    item['userEducationTopicDataToken'] = ''
                    item['userEducationTopicId'] = ''
                except:
                    item['userEducationTopicLinkId'] = ''
                    item['userEducationTopicDataToken'] = ''
                    item['userEducationTopicId'] = ''
                    item['userEducationText'] = ''


                # item['userEducationTopicLink'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="education"]//span[contains(@class,"education")]/a/@href').extract()[0]
                # item['userEducationTopicDataToken'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="education"]//span[contains(@class,"education")]/a/@data-token').extract()[0]
                # item['userEducationTopicId'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="education"]//span[contains(@class,"education")]/a/@data-topicid').extract()[0]
                # item['userEducationText'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="education"]//span[contains(@class,"education")]/a/text()').extract()[0]
            try:
                selEduExtra = selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="education"]//span[contains(@class,"education-extra")]/a')
                item['userEducationExtraLinkId'] = selEduExtra.xpath('@href').re(r'/topic/(\d+)')[0]
                item['userEducationExtraTopicDataToken'] = selEduExtra.xpath('@data-token').extract()[0]
                item['userEducationExtraTopicId'] = selEduExtra.xpath('@data-topicid').extract()[0]
                item['userEducationExtraText'] = selEduExtra.xpath('text()').extract()[0]
            except:
                try:
                    item['userEducationExtraText'] = selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="education"]//span[contains(@class,"education-extra")]/text()').extract()[0]
                    item['userEducationExtraLinkId'] = ''
                    item['userEducationExtraTopicDataToken'] = ''
                    item['userEducationExtraTopicId'] = ''

                except:
                    item['userEducationExtraLinkId'] = ''
                    item['userEducationExtraTopicDataToken'] = ''
                    item['userEducationExtraTopicId'] = ''
                    item['userEducationExtraText'] = ''

                # item['userEducationExtraLink'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="education"]//span[contains(@class,"education-extra")]/a/@href').extract()[0]
                # item['userEducationExtraTopicDataToken'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="education"]//span[contains(@class,"education-extra")]/a/@data-token').extract()[0]
                # item['userEducationExtraTopicId'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="education"]//span[contains(@class,"education-extra")]/a/@data-topicid').extract()[0]
                # item['userEducationExtraText'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"body")]/div[contains(@class,"zm-profile-header-info")]//div[@data-name="education"]//span[contains(@class,"education-extra")]/a/text()').extract()[0]

            try:
                item['userDescriptionText'] = '\n\n'.join(selProfile.xpath('div[contains(@class,"zm-profile-header-user-describe")]//div[@data-name="description"]//span[contains(@class,"unfold-item")]/span[@class="content"]/text()').extract())
            except:
                item['userDescriptionText'] = ''


            # item['userAgreeCount'] = response.xpath('//div[@class="zm-profile-header-info-list"]//span[@class="zm-profile-header-user-agree"]/strong/text()').extract()[0]
            # item['userThanksCount'] = response.xpath('//div[@class="zm-profile-header-info-list"]//span[@class="zm-profile-header-user-thanks"]/strong/text()').extract()[0]

            item['userDataId'] = response.xpath('//div[contains(@class,"zm-profile-header-op-btns")]//button/@data-id').extract()[0]

            try:
                item['userBoxId'] = response.xpath('//a[@id="zm-profile-header-pm-btn"]/@href').re(r'/inbox/(\d+)')[0]
            except:
                item['userBoxId'] =''
            item['userAskCount'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"profile-navbar")]/a[2]/span/text()').extract()[0]
            item['userAnswerCount'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"profile-navbar")]/a[3]/span/text()').extract()[0]
            item['userPostCount'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"profile-navbar")]/a[4]/span/text()').extract()[0]
            item['userCollectionCount'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"profile-navbar")]/a[5]/span/text()').extract()[0]
            item['userLogCount'] = response.xpath('//div[@class="zm-profile-header"]//div[contains(@class,"profile-navbar")]/a[6]/span/text()').extract()[0]

            item['userVoteCount'] = response.xpath('//div[contains(@class,"zm-profile-details-reputation")]/div[@class="zm-profile-module-desc"]/span[2]/strong/text()').extract()[0]
            item['userThanksCount'] = response.xpath('//div[contains(@class,"zm-profile-details-reputation")]/div[@class="zm-profile-module-desc"]/span[3]/strong/text()').extract()[0]
            item['userFavCount'] = response.xpath('//div[contains(@class,"zm-profile-details-reputation")]/div[@class="zm-profile-module-desc"]/span[4]/strong/text()').extract()[0]
            item['userShareCount'] = response.xpath('//div[contains(@class,"zm-profile-details-reputation")]/div[@class="zm-profile-module-desc"]/span[5]/strong/text()').extract()[0]


            item['userFolloweeCount'] = response.xpath('//div[contains(@class,"zu-main-sidebar")]/div[contains(@class,"zm-profile-side-following")]/a[1]/strong/text()').extract()[0]
            item['userFollowerCount'] = response.xpath('//div[contains(@class,"zu-main-sidebar")]/div[contains(@class,"zm-profile-side-following")]/a[2]/strong/text()').extract()[0]

            #这里需要验证
            if response.xpath('//div[contains(@class,"zu-main-sidebar")]//div[contains(@class,"zm-profile-side-columns")]/@class').extract():
                item['userColumnFollowingCount'] = response.xpath('//div[contains(@class,"zu-main-sidebar")]//div[2]//div[@class="zm-profile-side-section-title"]//a/strong/text()').re(r'(\d+)')[0]
                item['userTopicFollowingCount'] = response.xpath('//div[contains(@class,"zu-main-sidebar")]//div[3]//div[@class="zm-profile-side-section-title"]//a/strong/text()').re(r'(\d+)')[0]
            else:
                item['userColumnFollowingCount'] = 0
                item['userTopicFollowingCount'] = response.xpath('//div[contains(@class,"zu-main-sidebar")]//div[2]//div[@class="zm-profile-side-section-title"]//a/strong/text()').re(r'(\d+)')[0]
            item['userBrowsedTimes'] = response.xpath('//div[contains(@class,"zu-main-sidebar")]/div[last()-1]//span/strong/text()').extract()[0]
            yield item


            # if response.xpath('//div[contains(@class,"zu-main-sidebar")]//div[contains(@class,"zm-profile-side-topics")]/@class').extract():
            #     item['userTopicFollowedCount'] = response.xpath('//div[contains(@class,"zu-main-sidebar")]//div[2]//div[@class="zm-profile-side-section-title"]//a/strong/text()').re(r'(\d+)')[0]
            #     # item['userTopicFollowedCount'] = response.xpath('//div[contains(@class,"zu-main-sidebar")]//div[3]//div[@class="zm-profile-side-section-title"]//a/strong/text()').re(r'(\d+)')[0]
            # else:
            #     item['userTopicFollowedCount'] = 0



            # 这里剩下一块详细信息，职业经历，居住信息，教育经历,暂时放弃
            #item['userFollower'] = response.xpath('//div[@id="zh-pm-page-wrap"]/div[contains(@class,"zm-profile-section-list")]/div[2]//div[@class="zm-profile-details-item-detail"]/strong/a/text()').extract()[0]




        # for index,url in enumerate(self.urls):
        #     yield Request(url,meta = {'cookiejar':index},callback = self.parse_page)




    def closed(self,reason):
        self.client = MongoClient(settings.MONGO_URL)
        self.db = self.client['zhihu']
        self.col_log = self.db['log']

        crawler_log = {'project':settings.BOT_NAME,
                       'spider':self.name,
                       'spider_type':self.spider_type,
                       'spider_number':self.spider_number,
                       'partition':self.partition,
                       'type':'close',
                       'stats':self.stats.get_stats(),
                       'updated_at':datetime.datetime.now()}

        self.col_log.insert_one(crawler_log)
        # redis15 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=15)
        #
        #
        # #这样的顺序是为了防止两个几乎同时结束
        # p15=redis15.pipeline()
        # p15.lpush(str(self.name),self.spider_number)
        # p15.llen(str(self.name))
        # finishedCount= p15.execute()[1]
        #
        # if int(self.partition)==int(finishedCount):
        #     #删除其他标记
        #     redis15.ltrim(str(self.name),0,0)
        #
        #     #清空队列
        #     redis15.rpop(self.name)
        #     #清空缓存数据的redis11数据库
        #     payload=settings.NEXT_SCHEDULE_PAYLOAD
        #     logging.warning('Begin to request next schedule')
        #     response = requests.post('http://'+settings.NEXT_SCHEDULE_SCRAPYD_HOST+':'+settings.NEXT_SCHEDULE_SCRAPYD_PORT+'/schedule.json',data=payload)
        #     logging.warning('Response: '+' '+str(response))
        # logging.warning('finished close.....')
