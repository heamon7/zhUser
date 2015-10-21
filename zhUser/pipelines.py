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
from pymongo import MongoClient

class UserInfoPipeline(object):
    def __init__(self):
        # self.redis5 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=5)
        # #redis8记录上次爬取用户动态的时间，
        # self.redis8 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=8)
        # connection = happybase.Connection(settings.HBASE_HOST)
        # self.userTable = connection.table('user')
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_user_info = db['UserInfo']

    def process_item(self, item, spider):
        if item['spiderName'] == 'userInfo':
            userDataId = str(item['userDataId'])
            currentTimestamp = int(time.time())
            # recordTimestamp = self.redis5.lindex(str(userDataId),0)
            recordTimestamp = None

            # if result:
            #     recordTimestamp =result
            # else:
            #     recordTimestamp=''
            #为userActivity做准备
            #如果没有记录过该用户，那么将时间标记为0
            # if not recordTimestamp:
            #     p8 = self.redis8.pipeline()
            #     p8.lpush(str(userDataId)
            #              ,str(item['userLinkId'])
            #              ,str('0'))
            #     p8.ltrim(str(userDataId),0,2)
            #     p8.execute()
            # #如果记录过该用户，则将userLinkId更新
            # else:
            #     p8 = self.redis8.pipeline()
            #     p8.rpop(str(userDataId))
            #     p8.rpush(str(userDataId),str(item['userLinkId']))
            #     p8.execute()

            userGender = item['userGender']
            if userGender:
                userGender = 'f' if item['userGender'] == 'female' else 'm'

            if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp) > int(settings.INFO_UPDATE_PERIOD)):        # the latest record time in hbase
                recordTimestamp = currentTimestamp
                # p5 = self.redis5.pipeline()
                # p5.lpush(str(userDataId)
                #              ,str(item['userLinkId'])
                #              ,userGender
                #              ,str(item['userPostCount'])
                #              ,str(item['userLogCount'])
                #              ,str(item['userVoteCount'])
                #              ,str(item['userThanksCount'])
                #              ,str(item['userFavCount'])
                #              ,str(item['userShareCount'])
                #
                #              ,str(item['userCollectionCount'])
                #              ,str(item['userAnswerCount'])
                #              ,str(item['userAskCount'])
                #              ,str(item['userFolloweeCount'])
                #              ,str(item['userFollowerCount'])
                #              ,str(item['userColumnFollowingCount'])
                #              ,str(item['userTopicFollowingCount'])
                #              ,str(recordTimestamp))
                # p5.ltrim(str(userDataId),0,16)
                # p5.execute()

                user_detail_dict={
                    # 'user_link_id':str(item['userLinkId'].encode('utf-8')),
                    'user_link_id':str(item['userLinkId']),
                                'user_weibo_id':str(item['userWeiboId']),

                                'user_name': str(item['userName'].encode('utf-8')),
                               'user_bio': item['userBio'].encode('utf-8'),
                               'user_img_link': str(item['userImgLink']),
                                'user_gender': str(userGender),

                                'user_location_topic_link_id': str(item['userLocationTopicLinkId']),
                               'user_location_topic_data_token': item['userLocationTopicDataToken'],
                               'user_location_topic_id': str(item['userLocationTopicId']),
                               'user_location_name': str(item['userLocationText']).encode('utf-8'),

                                'user_business_topic_link_id': str(item['userBusinessTopicLinkId']),
                               'user_business_topic_data_token': item['userBusinessTopicDataToken'],
                               'user_business_topic_id': str(item['userBusinessTopicId']),
                               'user_business_topic_name': str(item['userBusinessText'].encode('utf-8')),

                                'user_employment_topic_link_id':str(item['userEmploymentTopicLinkId']),
                               'user_employment_topic_data_token':str(item['userEmploymentTopicDataToken']),
                               'user_employment_topic_id':str(item['userEmploymentTopicId']),
                               'user_employment_topic_name': str(item['userEmploymentText'].encode('utf-8')),

                               'user_position_topic_link_id': str(item['userPositionTopicLinkId']),
                               'user_position_topic_data_token': item['userPositionTopicDataToken'],
                               'user_position_topic_id': str(item['userPositionTopicId']),
                               'user_position_topic_name': str(item['userPositionText'].encode('utf-8')),

                               'user_education_topic_link_id': str(item['userEducationTopicLinkId']),
                               'user_education_topic_data_token': item['userEducationTopicDataToken'],
                               'user_education_topic_id': str(item['userEducationTopicId']),
                               'user_education_topic_name': str(item['userEducationText'].encode('utf-8')),

                               'user_education_extra_topic_link_id': item['userEducationExtraLinkId'],
                               'user_education_extra_topic_data_token': str(item['userEducationExtraTopicDataToken']),
                               'user_education_extra_topic_id': str(item['userEducationExtraTopicId']),
                               'user_education_extra_topic_name': str(item['userEducationExtraText'].encode('utf-8')),

                               'user_description_text':str(item['userDescriptionText'].encode('utf-8')),
                               'user_box_id':str(item['userBoxId']),
                               'user_ask_count':int(item['userAskCount']),
                               'user_answer_count': int(item['userAnswerCount']),
                               'user_post_count': int(item['userPostCount']),
                               'user_collection_count': int(item['userCollectionCount']),

                               'user_log_count': int(item['userLogCount']),
                               'user_vote_count': int(item['userVoteCount']),
                               'user_thanks_count': int(item['userThanksCount']),
                               'user_fav_count': int(item['userFavCount']),
                               'user_share_count': int(item['userShareCount']),
                               'user_followee_count': int(item['userFolloweeCount']),
                               'user_follower_count': int(item['userFollowerCount']),
                               'user_column_following_count': int(item['userColumnFollowingCount']),
                               'user_topic_following_count': int(item['userTopicFollowingCount']),
                               'user_browsed_times': int(item['userBrowsedTimes'])
                                }
                try:
                    self.col_user_info.insert_one(user_detail_dict)
                    # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                except Exception,e:
                    logging.warning('Error with put userInfo into mongo: '+str(e)+' try again......')
                    try:
                        self.col_user_info.insert_one(user_detail_dict)
                        # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                        logging.warning('tried again and successfully put data into redis ......')
                    except Exception,e:
                        logging.warning('Error with put userInfo into mongo: '+str(e)+'tried again and failed')
                        logging.error('The item is %s',str(item))
            return item

        else:
            return item

class UserFolloweePipeline(object):

    def __init__(self):
        # self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        # self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_user_followee = db['UserFollowee']
        self.col_user_link_id = db['UserLinkId']
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userFollowee':
            if item['userDataId'] and item['followeeDataId']:
                #userLinkId可能有中文
                followee_dict = {
                    'user_data_id':item['userDataId'],
                    'followee_data_id':item['followeeDataId']
                }
                try:
                    self.col_user_followee.insert_one(followee_dict)
                # 只要有关注行为的就记录下来
                    self.col_user_link_id.insert_one({'user_link_id':item['followeeLinkId']})
                # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                    # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                except Exception,e:
                    logging.warning('Error with put userFollowee into mongo: '+str(e)+' try again......')
                    try:
                        self.col_user_followee.insert_one(followee_dict)
                # 只要有关注行为的就记录下来
                        self.col_user_link_id.insert_one({'user_link_id':item['followeeLinkId']})
                # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                        # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                        logging.warning('tried again and successfully put data into redis ......')
                    except Exception,e:
                        logging.warning('Error with put userFollowee into mongo: '+str(e)+'tried again and failed')
                        logging.error('The item is %s',str(item))

            return item
        else:
            return item

class UserFollowerPipeline(object):

    def __init__(self):
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_user_follower = db['UserFollower']
        self.col_user_link_id = db['UserLinkId']
        # self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        # self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)

#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userFollower':
            if item['userDataId'] and item['followerDataId']:
                follower_dict = {
                    'user_data_id':item['userDataId'],
                    'follower_data_id':item['followerDataId']
                }
                try:
                    self.col_user_follower.insert_one(follower_dict)
                # 只要有关注行为的就记录下来
                    self.col_user_link_id.insert_one({'user_link_id':item['followerLinkId']})
                # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                    # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                except Exception,e:
                    logging.warning('Error with put userFollower into mongo: '+str(e)+' try again......')
                    try:
                        self.col_user_follower.insert_one(follower_dict)
                # 只要有关注行为的就记录下来
                        self.col_user_link_id.insert_one({'user_link_id':item['followerLinkId']})
                # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                        # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                        logging.warning('tried again and successfully put data into redis ......')
                    except Exception,e:
                        logging.warning('Error with put userFollower into mongo: '+str(e)+'tried again and failed')
                        logging.error('The item is %s',str(item))
                #userLinkId可能有中文
                # self.redis11.sadd(str(item['userDataId']),str(item['followerDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['followerLinkId'].encode('utf-8')))
            return item
        else:
            return item

class UserColumnPipeline(object):

    def __init__(self):
        # self.redis6 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=6)
        # self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        # connection = happybase.Connection(settings.HBASE_HOST)
        # self.columnTable = connection.table('column')
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_user_column = db['UserColumn']
        self.col_user_link_id = db['UserLinkId']

    def process_item(self, item, spider):

        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userColumn':
            if item['userDataId']:

                #既然redis6中已经存了，这里就不存了
                # self.redis3.sadd('columnLinkIdSet',columnLinkId)

                currentTimestamp = int(time.time())
                recordTimestamp = None

                # recordTimestamp = self.redis6.lindex(str(columnLinkId),0)
                # if result:
                #     recordTimestamp =result
                # else:
                #     recordTimestamp=''
                #无论之前有无记录，都会更新redis里的数据

                if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp) > int(settings.COLUMN_UPDATE_PERIOD)):        # the latest record time in hbase
                    recordTimestamp = currentTimestamp
                    # p6 = self.redis6.pipeline()
                    # p6.lpush(str(columnLinkId)
                    #              ,str(item['columnId'])
                    #              ,str(item['columnPostCount'])
                    #              ,str(recordTimestamp))
                    # p6.ltrim(str(columnLinkId),0,2)
                    # p6.execute()
                    column_basic_dict ={
                                'user_data_id':item['userDataId'],
                                  'column_img_link':item['columnImgLink'],
                                  'column_id':item['columnId'],
                                  'column_description':item['columnDescription'].encode('utf-8'),
                                  'column_post_count': item['columnPostCount'],
                                'column_link_id':item['columnLinkId'],
                                    }
                    try:
                        self.col_user_column.insert_one(column_basic_dict)
                        # self.redis6.lset(str(columnLinkId),0,str(recordTimestamp))
                    except Exception,e:
                        logging.warning('Error with put columnLinkId into mongo: '+str(e)+' try again......')
                        try:
                            self.col_user_column.insert_one(column_basic_dict)
                            # self.redis6.lset(str(columnLinkId),0,str(recordTimestamp))
                            logging.warning('tried again and successfully put data into redis ......')
                        except Exception,e:
                            logging.error('Error with put columnLinkId into mongo: '+str(e)+'tried again and failed')
                            logging.error('The item is %s',str(item))
            return item
        else:
            return item

class UserTopicPipeline(object):

    def __init__(self):
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_user_topic = db['UserTopic']
        # self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        # self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userTopic':
            if item['userDataId']:
                topic_basic_dict = {
                    'user_data_id':item['userDataId'],
                    'topic_link_id':item['topicLinkId'],
                    'topic_answer_count':int(item['topicAnswerCount'])
                }
                try:
                    self.col_user_topic.insert_one(topic_basic_dict)
                # 只要有关注行为的就记录下来
                # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                    # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                except Exception,e:
                    logging.warning('Error with put UserTopic into mongo: '+str(e)+' try again......')
                    try:
                        self.col_user_topic.insert_one(topic_basic_dict)
                # 只要有关注行为的就记录下来
                # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                        # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                        logging.warning('tried again and successfully put data into redis ......')
                    except Exception,e:
                        logging.warning('Error with put UserTopic into mongo: '+str(e)+'tried again and failed')
                        logging.error('The item is %s',str(item))
                # topicLinkId = item['topicLinkId']
                # self.redis3.sadd('topicIdSet',topicLinkId)
                # #存放用户在其关注的每个话题下得回答数量
                # self.redis11.lpush(str(item['userDataId']),
                #                    topicLinkId,
                #                    item['topicAnswerCount'])
            return item
        else:
            return item

class UserAskPipeline(object):

    def __init__(self):
        # self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        # self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        # connection = happybase.Connection(settings.HBASE_HOST,timeout=10000)
        # self.questionTable = connection.table('question')
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_user_ask = db['UserAsk']
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userAsk':
            if item['userDataId']:
                #这里需要在question那个表里添加一个提问者的信息么
                user_ask_dict = {
                    'user_data_id':item['userDataId'],
                    'ques_id':item['questionId']

                }
                try:
                    self.col_user_ask.insert_one(user_ask_dict)
                # 只要有关注行为的就记录下来
                # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                    # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                except Exception,e:
                    logging.warning('Error with put UserAsk into mongo: '+str(e)+' try again......')
                    try:
                        self.col_user_ask.insert_one(user_ask_dict)
                # 只要有关注行为的就记录下来
                # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                        # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                        logging.warning('tried again and successfully put data into redis ......')
                    except Exception,e:
                        logging.warning('Error with put UserAsk into mongo: '+str(e)+'tried again and failed')
                        logging.error('The item is %s',str(item))




                # self.redis11.sadd(str(item['userDataId']),item['questionId'])
                # 这里考虑
                # self.questionTable.put(str())
            return item
        else:
            return item

class UserAnswerPipeline(object):

    def __init__(self):
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_user_answer = db['UserAnswer']
        # self.redis3 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=3)
        # self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userAnswer':
            if item['userDataId']:
                user_answer_dict = {
                    'user_data_id':item['userDataId'],
                    'ques_id':item['questionId'],
                    'answer_data_token':item['answerDataToken']
                }
                try:
                    self.col_user_answer.insert_one(user_answer_dict)
                # 只要有关注行为的就记录下来
                # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                    # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                except Exception,e:
                    logging.warning('Error with put UserAnswer into mongo: '+str(e)+' try again......')
                    try:
                        self.col_user_answer.insert_one(user_answer_dict)
                # 只要有关注行为的就记录下来
                # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                        # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                        logging.warning('tried again and successfully put data into redis ......')
                    except Exception,e:
                        logging.warning('Error with put UserAnswer into mongo: '+str(e)+'tried again and failed')
                        logging.error('The item is %s',str(item))
                #理论上来讲，是不会在一次抓取过程中有重复的，为了便于取，这里存为列表
                # self.redis11.lpush(str(item['userDataId']),
                #                    item['questionId'],
                #                    item['answerDataToken'])
            return item
        else:
            return item

class UserCollectionPipeline(object):

    def __init__(self):
        # self.redis7 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=7)
        # self.redis11 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=11)
        # connection = happybase.Connection(settings.HBASE_HOST)
        # self.collectionTable = connection.table('collection')
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_user_collection = db['UserCollection']
#这里简单处理，不考虑关注者的前后顺序，处理为一个集合,每个关注在数据库里存为一条记录，在缓存里存为一个hash表
    def process_item(self, item, spider):
        #这里只取用户的linkId作为下一步userInfo的源，userDataId只是存到questionFollower里
        if item['spiderName'] == 'userCollection':
            if item['userDataId']:
                collectionLinkId = item['collectionLinkId']
                #为了可读性，这里并不使用只有数字的id
                # collectionFvId = re.split('fv\-(\d+)',item['collectionFvId'])[1]

                # self.redis3.sadd('columnLinkIdSet',columnLinkId)
                # self.redis11.sadd(str(item['userDataId']),collectionLinkId)
                currentTimestamp = int(time.time())
                # recordTimestamp = self.redis7.lindex(str(collectionLinkId),0)
                recordTimestamp = None
                # if result:
                #     recordTimestamp =result
                # else:
                #     recordTimestamp=''
                if not recordTimestamp or (int(currentTimestamp)-int(recordTimestamp) > int(settings.COLLECTION_UPDATE_PERIOD)):        # the latest record time in hbase
                    recordTimestamp = currentTimestamp
                    # p7 = self.redis7.pipeline()
                    # p7.lpush(str(collectionLinkId)
                    #             ,str(collectionFvId)
                    #              ,str(item['collectionAnswerCount'])
                    #              ,str(item['collectionFollowerCount'])
                    #              ,str(recordTimestamp))
                    # p7.ltrim(str(collectionLinkId),0,3)
                    # p7.execute()
                    collection_basic_dict ={
                                  'user_data_id':item['userDataId'],
                                  'collection_name':item['collectionName'],
                                  'collection_fv_id':item['collectionFvId'],
                                  'collection_answer_count':item['collectionAnswerCount'],
                                  'collection_follower_count': item['collectionFollowerCount']
                                    }
                    try:
                        self.col_user_collection.insert_one(collection_basic_dict)
                # 只要有关注行为的就记录下来
                # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                    # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                    except Exception,e:
                        logging.warning('Error with put userCollection into mongo: '+str(e)+' try again......')
                        try:
                            self.col_user_collection.insert_one(collection_basic_dict)
                # 只要有关注行为的就记录下来
                # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
                # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                        # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                            logging.warning('tried again and successfully put data into redis ......')
                        except Exception,e:
                            logging.warning('Error with put userCollection into mongo: '+str(e)+'tried again and failed')
                            logging.error('The item is %s',str(item))


            return item

        else:
            return item

class UserActivityPipeline(object):

    def __init__(self):
        # self.redis8 = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, password=settings.REDIS_PASSWORD,db=8)
        # connection = happybase.Connection(settings.HBASE_HOST)
        # self.activityTable = connection.table('activity')
        client = MongoClient(settings.MONGO_URL)
        db = client['zhihu']
        self.col_user_activity = db['UserActivity']
    def process_item(self, item, spider):
        if item['spiderName'] == 'userActivity':
            if item['userDataId']:
                is_last_activity = 0
                if item['isLastActivity'] :
                    is_last_activity =1

                    #更新时间
                    # p8 = self.redis8.pipeline()
                    # p8.lpop(userDataId)
                    # p8.lpush(userDataId,item['userCurrentTimestamp'])
                    # p8.execute()
                activity_detail_dict = {
                    'user_data_id':item['userDataId'],
                    'user_activity_time':item['userActivityTime'],
                    'user_activity_type':item['userActivityType'],
                    'user_activity_link':item['userActivityLink'],
                    'is_last_activity':is_last_activity
                }
                try:
                    self.col_user_activity.insert_one(activity_detail_dict)
            # 只要有关注行为的就记录下来
            # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
            # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                except Exception,e:
                    logging.warning('Error with put UserActivity into mongo: '+str(e)+' try again......')
                    try:
                        self.col_user_activity.insert_one(activity_detail_dict)
            # 只要有关注行为的就记录下来
            # self.redis11.sadd(str(item['userDataId']),str(item['followeeDataId']))
            # self.redis3.sadd('userLinkIdSet',str(item['followeeLinkId'].encode('utf-8')))
                    # self.redis5.lset(str(userDataId),0,str(recordTimestamp))
                        logging.warning('tried again and successfully put data into redis ......')
                    except Exception,e:
                        logging.warning('Error with put UserActivity into mongo: '+str(e)+'tried again and failed')
                        logging.error('The item is %s',str(item))
                # try:
                #     self.activityTable.put(item['userDataId'],activityDetailDict,timestamp=int(item['userActivityTime']))
                # except Exception,e:
                #     logging.warning('Error with put activityDetailDict into hbase: '+str(e)+' try again......')
                #     try:
                #         self.activityTable.put(item['userDataId'],activityDetailDict,timestamp=int(item['userActivityTime']))
                #         logging.warning('tried again and successfully put data into redis ......')
                #     except Exception,e:
                #         logging.warning('Error with put activityDetailDict into hbase: '+str(e)+'tried again and failed')
            DropItem()
        else:
            DropItem()
