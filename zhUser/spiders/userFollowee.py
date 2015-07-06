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
from zhUser.items import UserFolloweeItem

class UserfolloweeSpider(scrapy.Spider):
    name = "userFollowee"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )
    baseUrl = 'http://www.zhihu.com/node/ProfileFolloweesListV2'

    userDataIdList = []
    userFolloweeCountList = []

    quesIndex =0
    reqLimit =20  # 后面请求的pagesize
    pipelineLimit = 100000
    threhold = 100
    handle_httpstatus_list = [401,429,500,502,504]
    params= '{"offset":%s,"order_by":"created","hash_id":"%s"}'

    def __init__(self,spider_type='Master',spider_number=0,partition=1,**kwargs):

        #所有user的DataId，LinkId对应关系存放在redis3里
        #但是注意，从comment里爬到的user是没有DataId的
        # userLinkId的来源有comment，voter，follower
        # 我们这里只抓取关注过问题，赞同过答案的user

        #redis5里以列表的形式存放了每个用户的信息
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
                    p5.lindex(str(userDataId),4)
                    if (index+1)%self.pipelineLimit ==0:
                        self.userFolloweeCountList.extend(p5.execute())
                    elif totalLength-index==1:
                        self.userFolloweeCountList.extend(p5.execute())

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
                    p5.lindex(str(userDataId),4)
                    if (index+1)%self.pipelineLimit ==0:
                        self.userFolloweeCountList.extend(p5.execute())
                    elif totalLength-index==1:
                        self.userFolloweeCountList.extend(p5.execute())

        elif self.spider_type =='Slave':
            logging.warning('Slave spider_type is '+self.spider_type)
            logging.warning('Slave number is '+str(self.spider_number) + ' partition is '+str(self.partition))
            if (self.partition-self.spider_number)!=1:
                self.userDataIdList = self.userDataIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.userDataIdList)
                for index ,userDataId in enumerate(self.userDataIdList):
                    p5.lindex(str(userDataId),4)
                    if (index+1)%self.pipelineLimit ==0:
                        self.userFolloweeCountList.extend(p5.execute())
                    elif totalLength-index==1:
                        self.userFolloweeCountList.extend(p5.execute())


            else:
                self.userDataIdList = self.userDataIdList[self.spider_number*totalLength/self.partition:]
                totalLength = len(self.userDataIdList)
                for index ,userDataId in enumerate(self.userDataIdList):
                    p5.lindex(str(userDataId),4)
                    if (index+1)%self.pipelineLimit ==0:
                        self.userFolloweeCountList.extend(p5.execute())
                    elif totalLength-index==1:
                        self.userFolloweeCountList.extend(p5.execute())

        else:
            logging.warning('spider_type is:'+str(self.spider_type)+'with type of '+str(type(self.spider_type)))

        logging.warning('start_requests ing ......')
        logging.warning('totalCount to request is :'+str(len(self.userDataIdList)))
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

        for index ,userDataId in enumerate(self.userDataIdList):

            xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]

            reqTimes = (int(self.userFolloweeCountList[index])+self.reqLimit-1)/self.reqLimit
            reqUrl = self.baseUrl
            for index in reversed(range(reqTimes)):
                offset =str(self.reqLimit*index)
                reqParams = self.params %(str(offset),str(userDataId))
                yield FormRequest(url =reqUrl
                                  ,meta={'params':reqParams
                                    ,'xsrfValue':xsrfValue
                                    ,'userDataId':userDataId
                                    ,'offset':offset}
                                  , formdata={
                                        'method':'next'
                                        ,'params':reqParams
                                        ,'_xsrf': xsrfValue

                                    }
                                  ,dont_filter=True
                                  ,callback=self.parsePage
                                  )







    def parsePage(self,response):

        if response.status != 200:
            yield FormRequest(url =response.request.url,
                                              #headers = self.headers,
                                              meta={'params':response.meta['params']
                                                  ,'xsrfValue':response.meta['xsrfValue']
                                                  ,'userDataId':response.meta['userDataId']
                                                  ,'offset':response.meta['offset']},
                                              formdata={
                                                  'method':'next',
                                                  'params':response.meta['params'],
                                                  '_xsrf':response.meta['xsrfValue'],

                                              },
                                              dont_filter = True,
                                              callback = self.parsePage
                                              )
        else:
            item =  UserFolloweeItem()
            data = json.loads(response.body)
            followeeList = data['msg']

            item['spiderName'] = self.name
            #这里注意要处理含有匿名用户的情况
            if followeeList:

                res = Selector(text = ''.join(followeeList))
                item['userDataId'] = response.meta['userDataId']
                item['offset'] = response.meta['offset']

                for sel in res.xpath('//div[contains(@class,"zm-profile-card")]'):
                    try:
                        item['followeeDataId'] = sel.xpath('div[@class="zg-right"]/button/@data-id').extract()[0]
                        # 注意userLinkId中可能有中文
                        item['followeeLinkId'] = sel.xpath('a[@class="zm-item-link-avatar"]/@href').re(r'/people/(.*)')[0]
                    except:
                        item['followeeDataId'] = ''

                    yield item
                    #工作量太大，暂时并不需要这些详细信息，这需要Link，然后由userInfo的crawler抓取

                    # item['userImgLink'] = sel.xpath('a[contains(@class,"zm-item-link-avatar")]/img/@href').extract()[0]
                    #
                    # item['userName'] = sel.xpath('div[@class="body"]//a[@class="zg-link"]/text()').extract()[0]
                    # item['userAgreeCount'] = sel.xpath('div[@class="body"]//ul[@class="status"]/li[1]/span/text()').re('(\d+)')[0]
                    # item['userThanksCount'] = sel.xpath('div[@class="body"]//ul[@class="status"]/li[2]/span/text()').re('(\d+)')[0]
                    # item['userAskCount'] = sel.xpath('div[@class="body"]//ul[@class="status"]/li[3]/a/text()').re('(\d+)')[0]
                    # item['userAnswerCount'] = sel.xpath('div[@class="body"]//ul[@class="status"]/li[4]/a/text()').re('(\d+)')[0]

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
            answerTable = connection.table('user')

            userDataIdList = redis11.keys()
            p11 = redis11.pipeline()
            tmpUserList = []
            totalLength = len(userDataIdList)

            for index, userDataId in enumerate(userDataIdList):
                p11.smembers(str(userDataId))
                tmpUserList.append(str(userDataId))

                if (index + 1) % pipelineLimit == 0:
                    userFolloweeDataIdSetList = p11.execute()
                    with  answerTable.batch(batch_size=batchLimit):
                        for innerIndex, userFolloweeDataIdSet in enumerate(userFolloweeDataIdSetList):

                            answerTable.put(str(tmpUserList[innerIndex]),
                                              {'followee:dataIdList': str(list(userFolloweeDataIdSet))})
                        tmpUserList=[]

                elif  totalLength - index == 1:
                    userFolloweeDataIdSetList = p11.execute()
                    with  answerTable.batch(batch_size=batchLimit):
                        for innerIndex, userFolloweeDataIdSet in enumerate(userFolloweeDataIdSetList):

                            answerTable.put(str(tmpUserList[innerIndex]),
                                              {'followee:dataIdList': str(list(userFolloweeDataIdSet))})
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
