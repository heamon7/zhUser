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

from zhUser import settings
from zhUser.items import UserAnswerItem

class UserAnswerSpider(scrapy.Spider):
    name = "userAnswer"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )
    baseUrl = 'http://www.zhihu.com/people/%s/answers?page=%s'

    userLinkIdList = []
    userAnswerCountList = []

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
                    p5.lindex(str(userDataId),5)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userAnswerCountList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userAnswerCountList.extend(result[1::2])


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
                    p5.lindex(str(userDataId),5)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userAnswerCountList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userAnswerCountList.extend(result[1::2])

        elif self.spider_type =='Slave':
            logging.warning('Slave spider_type is '+self.spider_type)
            logging.warning('Slave number is '+str(self.spider_number) + ' partition is '+str(self.partition))
            if (self.partition-self.spider_number)!=1:
                self.userDataIdList = self.userDataIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.userDataIdList)
                for index ,userDataId in enumerate(self.userDataIdList):
                    p5.lindex(str(userDataId),-1)
                    p5.lindex(str(userDataId),5)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userAnswerCountList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userAnswerCountList.extend(result[1::2])


            else:
                self.userDataIdList = self.userDataIdList[self.spider_number*totalLength/self.partition:]
                totalLength = len(self.userDataIdList)
                for index ,userDataId in enumerate(self.userDataIdList):
                    p5.lindex(str(userDataId),-1)
                    p5.lindex(str(userDataId),5)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userAnswerCountList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p5.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userAnswerCountList.extend(result[1::2])

        else:
            logging.warning('spider_type is:'+str(self.spider_type)+'with type of '+str(type(self.spider_type)))

        logging.warning('start_requests ing ......')
        logging.warning('totalCount to request is :'+str(len(self.userDataIdList)))


        for index ,userLinkId in enumerate(self.userLinkIdList):

            reqTimes = (int(self.userAnswerCountList[index])+self.reqLimit-1)/self.reqLimit

            userDataId = self.userDataIdList[index]
            for index in reversed(range(reqTimes)):
                reqUrl = self.baseUrl %(str(userLinkId),str(index+1))
                yield Request(url =reqUrl
                                  ,meta={'userLinkId':userLinkId
                                    ,'userDataId':userDataId
                                    ,'page':str(index+1)}
                                  ,callback=self.parsePage
                                  )










    def parsePage(self,response):

        if response.status != 200:
            yield Request(url =response.request.url,
                                              #headers = self.headers,
                                              meta={'userLinkId':response.meta['userLinkId']
                                                  ,'userDataId':response.meta['userDataId']
                                                  ,'page':response.meta['page']},
                                              callback = self.parsePage
                                              )
        else:
            item =  UserAnswerItem()
            item['spiderName'] = self.name
            sels= response.xpath('//div[@id="zh-profile-answer-list"]/div[contains(@class,"zm-item")]')
            if sels:

                item['userLinkId'] = response.meta['userLinkId']
                item['userDataId'] = response.meta['userDataId']
                item['page'] = response.meta['page']

                for sel in sels:
                    #answerDataToken是唯一标示符,但为了方便，同时保存answer的questionId
                    item['questionId'] = sel.xpath('h2/a[@class="question_link"]/@href').re(r'/question/(\d+)/answer/')[0]
                    item['answerDataToken'] = sel.xpath('div[contains(@class,"zm-item-answer")]/@data-atoken').extract()[0]

                    #miid实际上是创建时间
                    # item['answerMiId'] = sel.xpath('@id').extract()[0]
                    # item['answerLink'] = sel.xpath('h2/a[@class="question_link"]/@href').extract()[0]
                    # item['questionTitle'] = sel.xpath('h2/a[@class="question_link"]/text()').extract()[0]

                    # item['answerDataId'] = sel.xpath('div[contains(@class,"zm-item-answer")]/@data-aid').extract()[0]
                    # item['answerDataToken'] = sel.xpath('div[contains(@class,"zm-item-answer")]/@data-atoken').extract()[0]
                    # item['answerDataCreated'] = sel.xpath('div[contains(@class,"zm-item-answer")]/@data-created').extract()[0]
                    # 其他的数据貌似没有意义
                    # item['answerDataToken'] = sel.xpath('div[contains(@class,"zm-item")]/div[contains(@class,"zm-item-answer")]/@data-atoken').extract()
                    # item['answerMiId'] = sel.xpath('div[contains(@class,"zm-item")]/@id').extract()
                    # item['answerMiId'] = sel.xpath('div[contains(@class,"zm-item")]/@id').extract()
                    # item['answerMiId'] = sel.xpath('div[contains(@class,"zm-item")]/@id').extract()
                    #
                    # item['userDataId'] = sel.xpath('div[@class="zm-profile-section-main"]//a[@class="question_link"]/@href').re(r'/question/(.*)')[0]
                    # item['questionTitle'] = sel.xpath('div[@class="zm-profile-section-main"]//a[@class="question_link"]/text()').extract()[0]
                    #
                    # item['questionSId'] = sel.xpath('div[@class="zm-profile-section-main"]/div[contains(@class,"meta")]/a/@id').extract()[0]
                    #
                    # item['questionAnswerCount'] = sel.xpath('div[@class="zm-profile-section-main"]/div[contains(@class,"meta")]/text()')[2].re(r'(\d+)')[0]
                    # item['questionFollowerCount'] = sel.xpath('div[@class="zm-profile-section-main"]/div[contains(@class,"meta")]/text()')[3].re(r'(\d+)')[0]


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
                p11.hgetall(str(userDataId))
                tmpUserList.append(str(userDataId))

                if (index + 1) % pipelineLimit == 0:
                    userAnswerLinkIdDictList = p11.execute()
                    with  userTable.batch(batch_size=batchLimit):
                        for innerIndex, userAnswerLinkIdDict in enumerate(userAnswerLinkIdDictList):
                            userTable.put(str(tmpUserList[innerIndex]),
                                              {'answer:linkIdDict': str(list(userAnswerLinkIdDict))})
                        tmpUserList=[]


                elif  totalLength - index == 1:
                    userAnswerLinkIdDictList = p11.execute()
                    with  userTable.batch(batch_size=batchLimit):
                        for innerIndex, userAnswerLinkIdDict in enumerate(userAnswerLinkIdDictList):
                            userTable.put(str(tmpUserList[innerIndex]),
                                              {'answer:linkIdDict': str(list(userAnswerLinkIdDict))})
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