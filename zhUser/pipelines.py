# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
from scrapy.exceptions import DropItem

from zhUser import settings
import time
import re
import redis
import happybase

class UserInfoPipeline(object):
    def __init__(self):
        self.redis5 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=5)
        #redis8记录上次爬取用户动态的时间，
        self.redis8 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=8)
        connection = happybase.Connection(settings.HBASE_HOST)
        self.userTable = connection.table('user')

    def process_item(self, item, spider):
        if item['spiderName'] == 'userInfo':
            userDataId = str(item['userDataId'])
            currentTimestamp = int(time.time())
            recordTimestamp = self.redis5.lindex(str(userDataId),0)
            # if result:
            #     recordTimestamp =result
            # else:
            #     recordTimestamp=''
            #为userActivity做准备
            if not recordTimestamp:
                self.redis8.lpush(str(userDataId)
                         ,str(item['userLinkId'])
                         ,str('0'))

            userGender = item['userGender']
            if userGender:
                userGender = 'f' if item['userGender'] == 'female' else 'm'

            if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp) > int(settings.INFO_UPDATE_PERIOD)):        # the latest record time in hbase
                recordTimestamp = currentTimestamp
                p5 = self.redis5.pipeline()
                p5.lpush(str(userDataId)
                             ,str(item['userLinkId'])
                             ,userGender
                             ,str(item['userPostCount'])
                             ,str(item['userLogCount'])
                             ,str(item['userVoteCount'])
                             ,str(item['userThanksCount'])
                             ,str(item['userFavCount'])
                             ,str(item['userShareCount'])

                             ,str(item['userCollectionCount'])
                             ,str(item['userAnswerCount'])
                             ,str(item['userAskCount'])
                             ,str(item['userFolloweeCount'])
                             ,str(item['userFollowerCount'])
                             ,str(item['userColumnFollowingCount'])
                             ,str(item['userTopicFollowingCount'])
                             ,str(recordTimestamp))
                p5.ltrim(str(userDataId),0,16)
                p5.execute()
                userDetailDict={'detail:userLinkId':str(item['userLinkId'].encode('utf-8')),
                                'detail:userWeiboId':str(item['userWeiboId']),

                                'detail:userName': str(item['userName'].encode('utf-8')),
                               'detail:userBio': item['userBio'].encode('utf-8'),
                               'detail:userImgLink': str(item['userImgLink']),
                                'detail:userGender': str(userGender),

                                'detail:userLocationTopicLinkId': str(item['userLocationTopicLinkId']),
                               'detail:userLocationTopicDataToken': item['userLocationTopicDataToken'],
                               'detail:userLocationTopicId': str(item['userLocationTopicId']),
                               'detail:userLocationText': str(item['userLocationText'].encode('utf-8')),

                                'detail:userBusinessTopicLinkId': str(item['userBusinessTopicLinkId']),
                               'detail:userBusinessTopicDataToken': item['userBusinessTopicDataToken'],
                               'detail:userBusinessTopicId': str(item['userBusinessTopicId']),
                               'detail:userBusinessText': str(item['userBusinessText'].encode('utf-8')),

                                'detail:userEmploymentTopicLinkId':str(item['userEmploymentTopicLinkId']),
                               'detail:userEmploymentTopicDataToken':str(item['userEmploymentTopicDataToken']),
                               'detail:userEmploymentTopicId':str(item['userEmploymentTopicId']),
                               'detail:userEmploymentText': str(item['userEmploymentText'].encode('utf-8')),

                               'detail:userPositionTopicLinkId': str(item['userPositionTopicLinkId']),
                               'detail:userPositionTopicDataToken': item['userPositionTopicDataToken'],
                               'detail:userPositionTopicId': str(item['userPositionTopicId']),
                               'detail:userPositionText': str(item['userPositionText'].encode('utf-8')),

                               'detail:userEducationTopicLinkId': str(item['userEducationTopicLinkId']),
                               'detail:userEducationTopicDataToken': item['userEducationTopicDataToken'],
                               'detail:userEducationTopicId': str(item['userEducationTopicId']),
                               'detail:userEducationText': str(item['userEducationText'].encode('utf-8')),

                               'detail:userEducationExtraLinkId': item['userEducationExtraLinkId'],
                               'detail:userEducationExtraTopicDataToken': str(item['userEducationExtraTopicDataToken']),
                               'detail:userEducationExtraTopicId': str(item['userEducationExtraTopicId']),
                               'detail:userEducationExtraText': str(item['userEducationExtraText'].encode('utf-8')),

                               'detail:userDescriptionText':str(item['userDescriptionText'].encode('utf-8')),
                               'detail:userBoxId':str(item['userBoxId']),
                               'detail:userAskCount':str(item['userAskCount']),
                               'detail:userAnswerCount': str(item['userAnswerCount']),
                               'detail:userPostCount': str(item['userPostCount']),
                               'detail:userCollectionCount': item['userCollectionCount'],

                               'detail:userLogCount': str(item['userLogCount']),
                               'detail:userVoteCount': str(item['userVoteCount']),
                               'detail:userThanksCount': str(item['userThanksCount']),
                               'detail:userFavCount': item['userFavCount'],
                               'detail:userShareCount': str(item['userShareCount']),
                               'detail:userFolloweeCount': str(item['userFolloweeCount']),
                               'detail:userFollowerCount': item['userFollowerCount'],
                               'detail:userColumnFollowingCount': str(item['userColumnFollowingCount']),
                               'detail:userTopicFollowingCount': str(item['userTopicFollowingCount']),
                               'detail:userBrowsedTimes': str(item['userBrowsedTimes'])
                                }
                try:
                    self.userTable.put(str(userDataId),userDetailDict)
                    # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                except Exception,e:
                    logging.warning('Error with put userDataId into redis: '+str(e)+' try again......')
                    try:
                        self.userTable.put(str(userDataId),userDetailDict)
                        # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                        logging.warning('tried again and successfully put data into redis ......')
                    except Exception,e:
                        logging.warning('Error with put userDataId into redis: '+str(e)+'tried again and failed')
            return item

        else:
            return item

class UserFolloweePipeline(object):

    def __init__(self):
        self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userFollowee':
            if item['userDataId'] and item['followeeDataId']:
                #userLinkId可能有中文
                self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
                self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
            return item
        else:
            return item

class UserFollowerPipeline(object):

    def __init__(self):
        self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userFollower':
            if item['userDataId'] and item['followerDataId']:
                #userLinkId可能有中文
                self.redis11.sadd(str(item['userDataId']),str(item['followerDataId']))
                self.redis3.sadd('userLinkIdSet',str(item['followerLinkId'].encode('utf-8')))
            return item
        else:
            return item

class UserColumnPipeline(object):

    def __init__(self):
        self.redis6 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=6)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        connection = happybase.Connection(settings.HBASE_HOST)
        self.columnTable = connection.table('column')

    def process_item(self, item, spider):

        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userColumn':
            if item['userDataId']:
                columnLinkId = item['columnLinkId']
                #既然redis6中已经存了，这里就不存了
                # self.redis3.sadd('columnLinkIdSet',columnLinkId)
                self.redis11.sadd(str(item['userDataId']),columnLinkId)
                currentTimestamp = int(time.time())
                recordTimestamp = self.redis6.lindex(str(columnLinkId),0)
                # if result:
                #     recordTimestamp =result
                # else:
                #     recordTimestamp=''
                #无论之前有无记录，都会更新redis里的数据

                if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp) > int(settings.COLUMN_UPDATE_PERIOD)):        # the latest record time in hbase
                    recordTimestamp = currentTimestamp
                    p6 = self.redis6.pipeline()
                    p6.lpush(str(columnLinkId)
                                 ,str(item['columnId'])
                                 ,str(item['columnPostCount'])
                                 ,str(recordTimestamp))
                    p6.ltrim(str(columnLinkId),0,2)
                    p6.execute()
                    columnBasicDict ={
                                  'basic:columnImgLink':item['columnImgLink'],
                                  'basic:columnId':item['columnId'],
                                  'basic:columnDescription':item['columnDescription'].encode('utf-8'),
                                  'basic:columnPostCount': item['columnPostCount']
                                    }
                    try:
                        self.columnTable.put(str(columnLinkId),columnBasicDict)
                        # self.redis6.lset(str(columnLinkId),0,str(recordTimestamp))
                    except Exception,e:
                        logging.warning('Error with put columnLinkId into redis: '+str(e)+' try again......')
                        try:
                            self.columnTable.put(str(columnLinkId),columnBasicDict)
                            # self.redis6.lset(str(columnLinkId),0,str(recordTimestamp))
                            logging.warning('tried again and successfully put data into redis ......')
                        except Exception,e:
                            logging.error('Error with put columnLinkId into redis: '+str(e)+'tried again and failed')
            return item
        else:
            return item

class UserTopicPipeline(object):

    def __init__(self):
        self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userTopic':
            if item['userDataId']:
                topicLinkId = item['topicLinkId']
                self.redis3.sadd('topicIdSet',topicLinkId)
                #存放用户在其关注的每个话题下得回答数量
                self.redis11.lpush(str(item['userDataId']),
                                   topicLinkId,
                                   item['topicAnswerCount'])
            return item
        else:
            return item

class UserAskPipeline(object):

    def __init__(self):
        # self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userAsk':
            if item['userDataId']:
                self.redis11.sadd(str(item['userDataId']),item['questionId'])
            return item
        else:
            return item

class UserAnswerPipeline(object):

    def __init__(self):
        self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userAnswer':
            if item['userDataId']:
                #理论上来讲，是不会在一次抓取过程中有重复的，为了便于取，这里存为列表
                self.redis11.lpush(str(item['userDataId']),
                                   item['questionId'],
                                   item['answerDataToken'])
            return item
        else:
            return item

class UserCollectionPipeline(object):

    def __init__(self):
        self.redis7 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=7)
        self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        connection = happybase.Connection(settings.HBASE_HOST)
        self.collectionTable = connection.table('collection')
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userCollection':
            if item['userDataId']:
                collectionLinkId = item['collectionLinkId']
                collectionFvId = re.split('fv\-(\d+)',item['collectionFvId'])[1]
                # self.redis3.sadd('columnLinkIdSet',columnLinkId)
                self.redis11.sadd(str(item['userDataId']),collectionLinkId)
                currentTimestamp = int(time.time())
                recordTimestamp = self.redis7.lindex(str(collectionLinkId),0)
                # if result:
                #     recordTimestamp =result
                # else:
                #     recordTimestamp=''
                if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp) > int(settings.COLLECTION_UPDATE_PERIOD)):        # the latest record time in hbase
                    recordTimestamp = currentTimestamp
                    p7 = self.redis7.pipeline()
                    p7.lpush(str(collectionLinkId)
                                ,str(collectionFvId)
                                 ,str(item['collectionAnswerCount'])
                                 ,str(item['collectionFollowerCount'])
                                 ,str(recordTimestamp))
                    p7.ltrim(str(collectionLinkId),0,3)
                    p7.execute()
                    collectionBasicDict ={
                                  'basic:userDataId':item['userDataId'],
                                  'basic:collectionName':item['collectionName'].encode('utf-8'),
                                  'basic:collectionFvId':collectionFvId,
                                  'basic:collectionAnswerCount':item['collectionAnswerCount'],
                                  'basic:collectionFollowerCount': item['collectionFollowerCount']
                                    }
                    try:
                        self.collectionTable.put(str(collectionLinkId),collectionBasicDict)
                        # self.redis7.lset(str(collectionLinkId),0,str(recordTimestamp))
                    except Exception,e:
                        logging.warning('Error with put collectionLinkId into hbase: '+str(e)+' try again......')
                        try:
                            self.collectionTable.put(str(collectionLinkId),collectionBasicDict)
                            # self.redis7.lset(str(collectionLinkId),0,str(recordTimestamp))
                            logging.warning('tried again and successfully put data into redis ......')
                        except Exception,e:
                            # logging.error('collectionBasicDict: %s',collectionBasicDict)
                            logging.error('Error with put collectionLinkId into hbase: '+str(e)+'tried again and failed')
            return item

        else:
            return item

class UserActivityPipeline(object):

    def __init__(self):
        self.redis8 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=8)
        connection = happybase.Connection(settings.HBASE_HOST)
        self.activityTable = connection.table('activity')
    def process_item(self, item, spider):
        if item['spiderName'] == 'userActivity':
            if item['userDataId']:
                userDataId = item['userDataId']
                if item['isLastActivity']:
                    p8 = self.redis8.pipeline()
                    p8.lpop(userDataId)
                    p8.lpush(userDataId,item['userCurrentTimestamp'])
                    p8.execute()
                activityDetailDict = {
                    'detail:userActivityTime':item['userActivityTime'],
                    'detail:userActivityType':item['userActivityType'],
                    'detail:userActivityLink':item['userActivityLink'],
                }
                try:
                    self.activityTable.put(item['userDataId'],activityDetailDict,timestamp=int(item['userActivityTime']))
                except Exception,e:
                    logging.warning('Error with put activityDetailDict into hbase: '+str(e)+' try again......')
                    try:
                        self.activityTable.put(item['userDataId'],activityDetailDict,timestamp=int(item['userActivityTime']))
                        logging.warning('tried again and successfully put data into redis ......')
                    except Exception,e:
                        logging.warning('Error with put activityDetailDict into hbase: '+str(e)+'tried again and failed')
            DropItem()
        else:
            DropItem()
