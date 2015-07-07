# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class UserInfoItem(scrapy.Item):
    spiderName = scrapy.Field()
    userLinkId = scrapy.Field()
    userWeiboId = scrapy.Field()
    userName = scrapy.Field()
    userBio = scrapy.Field()
    userImgLink = scrapy.Field()


    userLocationTopicLinkId = scrapy.Field()
    userLocationTopicDataToken = scrapy.Field()
    userLocationTopicId = scrapy.Field()
    userLocationText = scrapy.Field()

    userBusinessTopicLinkId = scrapy.Field()
    userBusinessTopicDataToken = scrapy.Field()
    userBusinessTopicId = scrapy.Field()
    userBusinessText = scrapy.Field()

    userGender = scrapy.Field()

    userEmploymentTopicLinkId = scrapy.Field()
    userEmploymentTopicDataToken = scrapy.Field()
    userEmploymentTopicId = scrapy.Field()
    userEmploymentText = scrapy.Field()

    userPositionTopicLinkId = scrapy.Field()
    userPositionTopicDataToken = scrapy.Field()
    userPositionTopicId = scrapy.Field()
    userPositionText = scrapy.Field()

    userEducationTopicLinkId = scrapy.Field()
    userEducationTopicDataToken = scrapy.Field()
    userEducationTopicId = scrapy.Field()
    userEducationText = scrapy.Field()

    userEducationExtraLinkId = scrapy.Field()
    userEducationExtraTopicDataToken = scrapy.Field()
    userEducationExtraTopicId = scrapy.Field()
    userEducationExtraText = scrapy.Field()

    userDescriptionText = scrapy.Field()
    userDataId = scrapy.Field()
    userBoxId = scrapy.Field()
    userAskCount = scrapy.Field()
    userAnswerCount = scrapy.Field()
    userPostCount = scrapy.Field()
    userCollectionCount = scrapy.Field()
    userLogCount = scrapy.Field()
    userVoteCount = scrapy.Field()
    userThanksCount = scrapy.Field()
    userFavCount = scrapy.Field()
    userShareCount = scrapy.Field()
    userFolloweeCount = scrapy.Field()
    userFollowerCount = scrapy.Field()
    userColumnFollowingCount = scrapy.Field()
    userTopicFollowingCount = scrapy.Field()
    userBrowsedTimes = scrapy.Field()



class UserFolloweeItem(scrapy.Item):
    spiderName = scrapy.Field()
    userDataId = scrapy.Field()
    offset = scrapy.Field()
    followeeDataId = scrapy.Field()
    followeeLinkId = scrapy.Field()




class UserFollowerItem(scrapy.Item):
    spiderName = scrapy.Field()
    userDataId = scrapy.Field()
    offset = scrapy.Field()
    followerDataId = scrapy.Field()
    followerLinkId = scrapy.Field()





class UserColumnItem(scrapy.Item):
    spiderName = scrapy.Field()
    userDataId = scrapy.Field()
    offset = scrapy.Field()
    userLinkId = scrapy.Field()
    columnLinkId = scrapy.Field()
    columnImgLink = scrapy.Field()
    columnId = scrapy.Field()
    columnDescription = scrapy.Field()
    columnPostCount = scrapy.Field()






class UserTopicItem(scrapy.Item):
    spiderName = scrapy.Field()
    userLinkId = scrapy.Field()
    userDataId = scrapy.Field()
    offset = scrapy.Field()
    topicLinkId = scrapy.Field()
    topicImgLink = scrapy.Field()
    topicId = scrapy.Field()
    topicName = scrapy.Field()
    topicAnswerCount = scrapy.Field()




class UserAskItem(scrapy.Item):

    spiderName = scrapy.Field()
    userLinkId = scrapy.Field()
    userDataId = scrapy.Field()
    page = scrapy.Field()
    questionId = scrapy.Field()




class UserAnswerItem(scrapy.Item):
    spiderName = scrapy.Field()
    userLinkId = scrapy.Field()
    userDataId = scrapy.Field()
    page = scrapy.Field()
    questionId = scrapy.Field()
    answerDataId = scrapy.Field()
    answerDataToken = scrapy.Field()





class UserCollectionItem(scrapy.Item):
    spiderName = scrapy.Field()
    userLinkId = scrapy.Field()
    userDataId = scrapy.Field()
    page = scrapy.Field()
    collectionLinkId = scrapy.Field()
    collectionName = scrapy.Field()
    collectionFvId = scrapy.Field()
    collectionAnswerCount = scrapy.Field()
    collectionFollowerCount = scrapy.Field()

class UserActivityItem(scrapy.Item):
    spiderName = scrapy.Field()
    userLinkId = scrapy.Field()
    userDataId = scrapy.Field()
    isLastActivity = scrapy.Field()
    userCurrentTimestamp = scrapy.Field()
    userActivityTime = scrapy.Field()
    userActivityType = scrapy.Field()
    userActivityClass = scrapy.Field()
    userActivityLink = scrapy.Field()





