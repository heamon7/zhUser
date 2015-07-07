# -*- coding: utf-8 -*-
import scrapy

from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request,FormRequest
from scrapy.selector import Selector
from scrapy.shell import inspect_response

from datetime import datetime
import re
import json
import redis
import requests
import logging
import happybase
import numpy as np
from zhUser import settings
from zhUser.items import UserTopicItem

class UserTopicSpider(scrapy.Spider):
    name = "userTopic"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )
    baseUrl = 'http://www.zhihu.com/people/%s/topics'

    userLinkIdList = []
    userTopicCountList = []

    quesIndex =0
    reqLimit =20  # 后面请求的pagesize
    pipelineLimit = 100000
    threhold = 100
    handle_httpstatus_list = [401,429,500,502,504]


    def __init__(self,spider_type='Master',spider_number=0,partition=1,**kwargs):

        #所有user的DataId，LinkId对应关系存放在redis3里
        #但是注意，从comment里爬到的user是没有DataId的
        # userLinkId的来源有comment，voter，follower
        # 我们这里只抓取关注过问题，赞同过答案的user
        self.redis5 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=5)
        self.spider_type = str(spider_type)
        self.spider_number = int(spider_number)
        self.partition = int(partition)
        self.email= settings.EMAIL_LIST[self.spider_number]
        self.password=settings.PASSWORD_LIST[self.spider_number]

    def start_requests(self):

        #这里需要userDataId和关注的人数
        self.userDataIdList = self.redis5.keys()
        totalLength = len(self.userDataIdList)

        p5 = self.redis5.pipeline()

        if self.spider_type=='Master':
            redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
            redis11.flushdb()
            logging.warning('Master spider_type is '+self.spider_type)
            if self.partition!=1:
                logging.warning('Master partition is '+str(self.partition))
                self.userDataIdList = self.userDataIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.userDataIdList)

                for index ,userDataId in enumerate(self.userDataIdList):
                    p5.lindex(str(userDataId),-1)
                    p5.lindex(str(userDataId),1)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userTopicCountList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userTopicCountList.extend(result[1::2])

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
                logging.warning('Master  partition is '+str(self.partition))
                for index ,userDataId in enumerate(self.userDataIdList):
                    p5.lindex(str(userDataId),-1)
                    p5.lindex(str(userDataId),1)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userTopicCountList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userTopicCountList.extend(result[1::2])

        elif self.spider_type =='Slave':
            logging.warning('Slave spider_type is '+self.spider_type)
            logging.warning('Slave number is '+str(self.spider_number) + ' partition is '+str(self.partition))
            if (self.partition-self.spider_number)!=1:
                self.userDataIdList = self.userDataIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.userDataIdList)

                for index ,userDataId in enumerate(self.userDataIdList):
                    p5.lindex(str(userDataId),-1)
                    p5.lindex(str(userDataId),1)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userTopicCountList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userTopicCountList.extend(result[1::2])


            else:
                self.userDataIdList = self.userDataIdList[self.spider_number*totalLength/self.partition:]
                totalLength = len(self.userDataIdList)
                for index ,userDataId in enumerate(self.userDataIdList):
                    p5.lindex(str(userDataId),-1)
                    p5.lindex(str(userDataId),1)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userTopicCountList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userTopicCountList.extend(result[1::2])

        else:
            logging.warning('spider_type is:'+str(self.spider_type)+'with type of '+str(type(self.spider_type)))

        logging.warning('start_requests ing ......')
        logging.warning('totalCount to request is :'+str(len(self.userDataIdList)))

        #
        # logging.warning('userDataIdList to request is :'+str(self.userDataIdList[0:5]))
        # logging.warning('userLinkIdList to request is :'+str(self.userLinkIdList[0:5]))
        # logging.warning('userTopicCountList to request is :'+str(self.userTopicCountList[0:5]))

        yield Request("http://www.zhihu.com/",callback = self.post_login)

    def post_login(self,response):

        logging.warning('post_login ing ......')
        xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]
        yield FormRequest.from_response(response,
                                          formdata={
                                              '_xsrf':xsrfValue,
                                              'email':self.email,
                                              'password':self.password,
                                              'rememberme': 'y'
                                          },
                                          dont_filter = True,
                                          callback = self.after_login,
                                          )

    def after_login(self,response):
        try:
            loginUserLink = response.xpath('//div[@id="zh-top-inner"]/div[@class="top-nav-profile"]/a/@href').extract()[0]
            logging.warning('Successfully login with %s  %s  %s',str(loginUserLink),str(self.email),str(self.password))
        except:
            logging.error('Login failed! %s   %s',self.email,self.password)

        for index ,userLinkId in enumerate(self.userLinkIdList):

            xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]

            reqTimes = (int(self.userTopicCountList[index])+self.reqLimit-1)/self.reqLimit
            reqUrl = self.baseUrl %str(userLinkId)
            userDataId = self.userDataIdList[index]
            for index in reversed(range(reqTimes)):
                offset =str(self.reqLimit*index)
                yield FormRequest(url =reqUrl
                                  ,meta={'xsrfValue':xsrfValue
                                    ,'userLinkId':userLinkId
                                    ,'userDataId':userDataId
                                    ,'offset':offset}
                                  , formdata={
                                        'start':'0'
                                        ,'offset':offset
                                        ,'_xsrf': xsrfValue

                                    }
                                  ,dont_filter=True
                                  ,callback=self.parsePage
                                  )







    def parsePage(self,response):

        if response.status != 200:
            yield FormRequest(url =response.request.url,
                                              #headers = self.headers,
                                              meta={'xsrfValue':response.meta['xsrfValue']
                                                  ,'userLinkId':response.meta['userLinkId']
                                                  ,'userDataId':response.meta['userDataId']
                                                  ,'offset':response.meta['offset']},
                                              formdata={
                                                  'start':'0',
                                                  'offset':response.meta['offset'],
                                                  '_xsrf':response.meta['xsrfValue'],
                                              },
                                              dont_filter = True,
                                              callback = self.parsePage
                                              )
        else:
            item =  UserTopicItem()
            item['spiderName'] = self.name
            data = json.loads(response.body)
            topicCount= int(data['msg'][0])
            if topicCount:
                topicList = data['msg'][1]
                #这里注意要处理含有匿名用户的情况
                res = Selector(text =topicList)
                item['userLinkId'] = response.meta['userLinkId']
                item['userDataId'] = response.meta['userDataId']
                item['offset'] = response.meta['offset']

                for sel in res.xpath('//div[contains(@class,"zm-profile-section-item")]'):
                    item['topicLinkId'] = sel.xpath('a[@class="zm-list-avatar-link"]/@href').re(r'/topic/(.*)')[0]
                    item['topicAnswerCount'] = sel.xpath('div[contains(@class,"zm-profile-section-main")]/div/a/text()').re(r'(\d+)')[0]

                    # item['topicImgLink'] = sel.xpath('a[@class="zm-list-avatar-link"]/img/@src').extract()[0]
                    # item['topicId'] = sel.xpath('div[contains(@class,"zm-profile-section-main")]/button/@id').extract()[0]
                    # item['topicName'] = sel.xpath('div[contains(@class,"zm-profile-section-main")]/a[@data-tip]/strong/text()').extract()[0]


                    yield item


            else:
                #没有用户
                item['userDataId']=''
                yield item


    def closed(self,reason):
        redis15 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=15)
        redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        #这样的顺序是为了防止两个几乎同时结束
        p15=redis15.pipeline()
        p15.lpush(str(self.name),self.spider_number)
        p15.llen(str(self.name))
        finishedCount= p15.execute()[1]
        pipelineLimit = 100000
        batchLimit = 1000

        if int(self.partition)==int(finishedCount):
            #删除其他标记
            redis15.ltrim(str(self.name),0,0)

            connection = happybase.Connection(settings.HBASE_HOST)
            userTable = connection.table('user')

            userDataIdList = redis11.keys()
            p11 = redis11.pipeline()
            tmpUserList = []
            totalLength = len(userDataIdList)

            for index, userDataId in enumerate(userDataIdList):
                p11.lrange(str(userDataId),0,-1)
                tmpUserList.append(str(userDataId))

                if (index + 1) % pipelineLimit == 0:
                    userTopicFollowingLinkIdCountListList = p11.execute()
                    with  userTable.batch(batch_size=batchLimit):
                        for innerIndex, userTopicFollowingLinkIdCountList in enumerate(userTopicFollowingLinkIdCountListList):
                            userTable.put(str(tmpUserList[innerIndex]),
                                              {'topic:linkIdCountList': str(list(userTopicFollowingLinkIdCountList))})
                        tmpUserList=[]


                elif  totalLength - index == 1:
                    userTopicFollowingLinkIdCountListList = p11.execute()
                    with  userTable.batch(batch_size=batchLimit):
                        for innerIndex, userTopicFollowingLinkIdCountList in enumerate(userTopicFollowingLinkIdCountListList):
                            userTable.put(str(tmpUserList[innerIndex]),
                                              {'topic:linkIdCountList': str(list(userTopicFollowingLinkIdCountList))})
                        tmpUserList=[]
            #清空队列
            redis15.rpop(self.name)
            #清空缓存数据的redis11数据库
            redis11.flushdb()

            payload=settings.NEXT_SCHEDULE_PAYLOAD
            logging.warning('Begin to request next schedule')
            response = requests.post('http://'+settings.NEXT_SCHEDULE_SCRAPYD_HOST+':'+settings.NEXT_SCHEDULE_SCRAPYD_PORT+'/schedule.json',data=payload)
            logging.warning('Response: '+' '+str(response))
        logging.warning('finished close.....')