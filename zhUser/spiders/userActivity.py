# -*- coding: utf-8 -*-
import scrapy

from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request,FormRequest
from scrapy.selector import Selector
from scrapy.shell import inspect_response

from datetime import datetime
import time
import re
import json
import redis
import requests
import logging

from zhUser import settings
from zhUser.items import UserActivityItem

class UserActivitySpider(scrapy.Spider):
    name = "userActivity"
    allowed_domains = ["zhihu.com"]
    start_urls = (
        'http://www.zhihu.com/',
    )
    baseUrl = 'http://www.zhihu.com/people/%s/activities'

    userLinkIdList = []
    userLastTimestampList = []

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
        self.redis8 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=8)

        try:
            self.spider_type = str(spider_type)
            self.spider_number = int(spider_number)
            self.partition = int(partition)
            self.email= settings.EMAIL_LIST[self.spider_number]
            self.password=settings.PASSWORD_LIST[self.spider_number]

        except:
            self.spider_type = 'Master'
            self.spider_number = 0
            self.partition = 1
            self.email= settings.EMAIL_LIST[self.spider_number]
            self.password=settings.PASSWORD_LIST[self.spider_number]

    def start_requests(self):

        #这里需要userDataId和关注的人数
        self.userDataIdList = self.redis8.keys()
        totalLength = len(self.userDataIdList)

        p8 = self.redis8.pipeline()

        if self.spider_type=='Master':
            redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
            redis11.flushdb()
            logging.warning('Master spider_type is '+self.spider_type)
            if self.partition!=1:
                logging.warning('Master partition is '+str(self.partition))
                self.userDataIdList = self.userDataIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.userDataIdList)
                for index ,userDataId in enumerate(self.userDataIdList):
                    p8.lindex(str(userDataId),-1)
                    p8.lindex(str(userDataId),0)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p8.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userLastTimestampList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p8.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userLastTimestampList.extend(result[1::2])




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
                    p8.lindex(str(userDataId),-1)
                    p8.lindex(str(userDataId),0)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p8.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userLastTimestampList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p8.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userLastTimestampList.extend(result[1::2])


        elif self.spider_type =='Slave':
            logging.warning('Slave spider_type is '+self.spider_type)
            logging.warning('Slave number is '+str(self.spider_number) + ' partition is '+str(self.partition))
            if (self.partition-self.spider_number)!=1:
                self.userDataIdList = self.userDataIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.userDataIdList)
                for index ,userDataId in enumerate(self.userDataIdList):
                    p8.lindex(str(userDataId),-1)
                    p8.lindex(str(userDataId),0)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p8.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userLastTimestampList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p8.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userLastTimestampList.extend(result[1::2])


            else:
                self.userDataIdList = self.userDataIdList[self.spider_number*totalLength/self.partition:(self.spider_number+1)*totalLength/self.partition]
                totalLength = len(self.userDataIdList)
                for index ,userDataId in enumerate(self.userDataIdList):
                    p8.lindex(str(userDataId),-1)
                    p8.lindex(str(userDataId),0)
                    if (index+1)%self.pipelineLimit ==0:
                        result = p8.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userLastTimestampList.extend(result[1::2])
                    elif totalLength-index==1:
                        result = p8.execute()
                        self.userLinkIdList.extend(result[0::2])
                        self.userLastTimestampList.extend(result[1::2])

        else:
            logging.warning('spider_type is:'+str(self.spider_type)+'with type of '+str(type(self.spider_type)))

        logging.warning('start_requests ing ......')
        logging.warning('totalCount to request is :'+str(len(self.userLinkIdList)))

        yield Request("http://www.zhihu.com",callback = self.post_login)


    def post_login(self,response):
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

        self.userCurrentTimestamp = int(time.time())




        for index ,userLinkId in enumerate(self.userLinkIdList):

            xsrfValue = response.xpath('/html/body/input[@name= "_xsrf"]/@value').extract()[0]


            reqUrl = self.baseUrl %str(userLinkId)

            userLastTimestamp = self.userLastTimestampList[index]

            yield FormRequest(url =reqUrl
                                  ,meta={'start':str(self.userCurrentTimestamp)
                                    ,'xsrfValue':xsrfValue
                                    ,'userDataId':self.userDataIdList[index]
                                    ,'userLastTimestamp': userLastTimestamp

                                         }
                                  , formdata={
                                        'start':str(self.userCurrentTimestamp)
                                        ,'_xsrf': xsrfValue

                                    }
                                  ,dont_filter=True
                                  ,callback=self.parsePage
                                  )


    def parsePage(self,response):

        if response.status != 200:
            yield FormRequest(url =response.request.url,
                                              #headers = self.headers,
                                              meta={'start':response.meta['start']
                                                  ,'xsrfValue':response.meta['xsrfValue']
                                                  ,'userDataId':response.meta['userDataId']
                                                  ,'userLastTimestamp': response.meta['userLastTimestamp']

                                                    },
                                              formdata={
                                                  'start':response.meta['start']
                                                  ,'_xsrf':response.meta['xsrfValue']

                                              },
                                              dont_filter = True,
                                              callback = self.parsePage
                                              )
        else:
            item =  UserActivityItem()
            data = json.loads(response.body)
            activityCount = int(data['msg'][0])
            item['spiderName'] = self.name
            #如果该用户有产生动态，那么统计，要考虑整除的情况
            if activityCount :

                # logging.warning('activityCount is: %s and response.url is: %s and start is: %s',activityCount,response.url,response.meta['start'])
                res = Selector(text = data['msg'][1])
                item['userDataId'] = response.meta['userDataId']
                # userLastTimestamp = response.meta['userLastTimestamp']
                # 如果是最后一批动态，注意要考虑整除状态

                if activityCount < self.reqLimit:
                    # logging.warning('come 1')

                    for index,sel in enumerate(res.xpath('//div[contains(@class,"zm-profile-section-item")]')):
                        # logging.warning('come 3')
                        # 这里相当于永远丢弃了最后一条activity，除非只有一条
                        # 如果该时间点之前的动态不只有一条，且如果迭代到最后一条
                        # 如果是最后一批动态的最后一条
                        if activityCount-index ==1:
                            # logging.warning('come 5')
                            #只在最后一次更新userLastTimestamp
                            item['isLastActivity'] = 1
                            item = self.extract(item,sel)
                            yield item
                        #最后一批动态的非最后一个动态
                        else:
                            # logging.warning('come 6')
                            item['isLastActivity'] = 0
                            item = self.extract(item,sel)
                            yield item
                #表明不是最后一批动态
                else:
                    # logging.warning('come 2')
                    for index,sel in enumerate(res.xpath('//div[contains(@class,"zm-profile-section-item")]')):
                        # logging.warning('come 4')
                        # 如果是非最后一批动态的最后一条
                        # 那么比较这批动态最后一条的时间和userLastTimestamp
                        if activityCount-index ==1:
                            # logging.warning('come 7')
                            curLastTimestamp = sel.xpath('@data-time').extract()[0]
                            if int(curLastTimestamp)>=int(response.meta['userLastTimestamp']):
                                yield FormRequest(url =response.request.url,
                                                  #headers = self.headers,
                                                  meta={'start':str(curLastTimestamp)
                                                      ,'xsrfValue':response.meta['xsrfValue']
                                                      ,'userDataId':response.meta['userDataId']
                                                    ,'userLastTimestamp': response.meta['userLastTimestamp']
                                                        },
                                                  formdata={
                                                      'start':str(curLastTimestamp)
                                                      ,'_xsrf':response.meta['xsrfValue']

                                                  },
                                                  dont_filter = True,
                                                  callback = self.parsePage
                                                  )
                            else:
                                # logging.warning('come 8')
                                item['isLastActivity'] = 1
                                item = self.extract(item,sel)
                                yield item
                        #非最后一批动态的非最后一个动态
                        else:
                            item['isLastActivity'] = 0

                            item = self.extract(item,sel)
                            # logging.warning('item content is: %s',item)
                            yield item

            #表示虽然没有返回动态，但是该用户有过动态，（用户的动态正好是self.reqLimit的整数倍）
            elif self.userCurrentTimestamp != int(response.meta['start']):
                #没有用户
                item['userDataId'] = response.meta['userDataId']
                item['isLastActivity'] = 1
                item['userCurrentTimestamp'] = self.userCurrentTimestamp
                yield item
            #表示该用户并没有产生过动态
            else:
                item['userDataId']=''
                yield item

    def extract(self,item,sel):
        item['userCurrentTimestamp'] = self.userCurrentTimestamp
        item['userActivityTime'] = sel.xpath('@data-time').extract()[0]
        item['userActivityLink'] = sel.xpath('div[contains(@class,"zm-profile-section-activity-main")]/a[2]/@href').extract()[0]
        try:
            item['userActivityType']  =  sel.xpath('@data-type').extract()[0]
        except:
            #出现了8种情况以外的异常，则过滤掉
            logging.error('Error in userActivity extract with item: %s and selector.extract: %s',item,sel.extract())
            item['userActivityType'] = ''
            item['userDataId']=''


        # try:
        #     item['userActivityType'] = sel.xpath('div[contains(@class,"zm-profile-section-activity-main")]/a[2]/@class').re(r'(\w*)_link')[0]
        # except:
        #     item['userActivityType'] = ''

        #如果是发表一篇专栏，那么sel.xpath('div/text()').extract()得到的第二个元素才是目标字符
        if item['userActivityType'] == 'p':
            item['userActivityType'] = '0'




        else:
            typeText = sel.xpath('div/text()').extract()[1]
            logging.warning('typeText: %s',typeText)
            # u' 赞同了回答'
            if typeText == u' \u8d5e\u540c\u4e86\u56de\u7b54\n\n':
                item['userActivityType'] = '1'
            #u' 关注了问题'
            elif typeText == u' \u5173\u6ce8\u4e86\u95ee\u9898\n\n':
                item['userActivityType'] = '2'
            #u' 回答了问题'
            elif typeText == u' \u56de\u7b54\u4e86\u95ee\u9898\n\n':
                item['userActivityType'] = '3'
            #u' 提了一个问题'
            elif typeText == u' \u63d0\u4e86\u4e00\u4e2a\u95ee\u9898\n\n':
                item['userActivityType'] = '4'
            #u' 关注了话题'
            elif typeText == u' \u5173\u6ce8\u4e86\u8bdd\u9898\n\n':
                item['userActivityType'] = '5'
            #u' 关注了专栏'
            elif typeText == u' \u5173\u6ce8\u4e86\u4e13\u680f\n\n':
                item['userActivityType'] = '6'
            #u' 关注了收藏夹'
            elif typeText == u' \u5173\u6ce8\u4e86\u6536\u85cf\u5939\n\n':
                item['userActivityType'] = '7'
            #u' 关注了圆桌'
            elif typeText == u' \u5173\u6ce8\u4e86\u5706\u684c\n\n':
                item['userActivityType'] = '8'
            else :
                logging.error('Error in userActivity typeText with  typeText: %s and selector.extract: %s',typeText,sel.extract().encode('utf-8'))
                item['userDataId']=''
            # item['userActivityLink'] = sel.xpath('div[contains(@class,"zm-profile-section-activity-main")]/a[2]/@href').extract()[0]

        #因为动态的数量是海量的，所以这里要尽一切可能减少存储空间
        # if not item['userActivityType'] :
        #     if item['userActivityClass'] == 'topic':
        #         item['userActivityLinkId'] = sel.xpath('div[contains(@class,"zm-profile-section-activity-main")]/a[2]/@href').re(r'/topic/(\d+)')[0]
        #         item['userActivityType'] = 't'
        #     elif item['userActivityClass'] == 'question':
        #         item['userActivityType'] = 'q'
        #         item['userActivityLinkId'] = sel.xpath('div[contains(@class,"zm-profile-section-activity-main")]/a[2]/@href').re(r'/question/(\d+)')[0]
        #     elif item['userActivityClass'] == 'column':
        #         item['userActivityType'] = 'c'
        #         item['userActivityLinkId'] = sel.xpath('div[contains(@class,"zm-profile-section-activity-main")]/a[2]/@href').re(r'http://zhuanlan.zhihu.com/(\d+)')[0]
        #     else:
        #         #error
        #         item['userActivityType'] = 'e'
        #         item['userActivityLinkId'] = sel.xpath('div[contains(@class,"zm-profile-section-activity-main")]/a[2]/@href').extract()[0]
        #         logging.error('Error in userActivity parsePage with item: %s and selector.extract: %s',item,sel.extract())
        # else:
        #     #表明这个activity是赞同，agree的a
        #     item['userActivityLinkId'] = ','.join(sel.xpath('div[contains(@class,"zm-profile-section-activity-main"/a[2]/@href').re('/question/(\d+)/answer/(\d+)'))
        return item


    def closed(self,reason):
        redis15 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=15)


        #这样的顺序是为了防止两个几乎同时结束
        p15=redis15.pipeline()
        p15.lpush(str(self.name),self.spider_number)
        p15.llen(str(self.name))
        finishedCount= p15.execute()[1]

        if int(self.partition)==int(finishedCount):
            #删除其他标记
            redis15.ltrim(str(self.name),0,0)

            #清空队列
            redis15.rpop(self.name)
            #清空缓存数据的redis11数据库
            payload=settings.NEXT_SCHEDULE_PAYLOAD
            logging.warning('Begin to request next schedule')
            response = requests.post('http://'+settings.NEXT_SCHEDULE_SCRAPYD_HOST+':'+settings.NEXT_SCHEDULE_SCRAPYD_PORT+'/schedule.json',data=payload)
            logging.warning('Response: '+' '+str(response))
        logging.warning('finished close.....')


    # item['userCurrentTimestamp'] = self.userCurrentTimestamp
                            # item['userActivityTime'] = sel.xpath('@data-time').extract()[0]
                            # item['userActivityType']  =  sel.xpath('@data-type').extract()[0]
                            # item['userActivityClass'] = sel.xpath('div[contains(@class,"zm-profile-section-activity-main"/a[2]/@class').re(r'(\w*)_link')[0]
                            # #因为动态的数量是海量的，所以这里要尽一切可能减少存储空间
                            # if not item['userActivityType'] :
                            #     if item['userActivityClass'] == 'topic':
                            #         item['userActivityLinkId'] = sel.xpath('div[contains(@class,"zm-profile-section-activity-main"/a[2]/@href').re(r'/topic/(\d+)')[0]
                            #         item['userActivityType'] = 't'
                            #     elif item['userActivityClass'] == 'question':
                            #         item['userActivityType'] = 'q'
                            #         item['userActivityLinkId'] = sel.xpath('div[contains(@class,"zm-profile-section-activity-main"/a[2]/@href').re(r'/question/(\d+)')[0]
                            #     elif item['userActivityClass'] == 'column':
                            #         item['userActivityType'] = 'c'
                            #         item['userActivityLinkId'] = sel.xpath('div[contains(@class,"zm-profile-section-activity-main"/a[2]/@href').re(r'http://zhuanlan.zhihu.com/(\d+)')[0]
                            #     else:
                            #         logging.error('Error in userActivityType')
                            # else:
                            #     #表明这个activity是赞同，agree的a
                            #     item['userActivityLinkId'] = ','.join(sel.xpath('div[contains(@class,"zm-profile-section-activity-main"/a[2]/@href').re('/question/(\d+)/answer/(\d+)'))