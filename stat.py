# -*- coding: UTF-8 -*-

from mysql_manager import mmysql
import _options
import datetime
import time

def stateInviteAndDownload():

    today = datetime.date.today()  
    oneday = datetime.timedelta(days=1)  
    yesterday = today - oneday   

    # print today
    #yesterday = "2015-06-25"

    mUser = mmysql(_options.db["db_user"])

    tmpLst = []

    for i in range(0,10): #get channel  used yesterday
            
        sql = "select channel from app_register_%d  where LEFT(rtime, 10) =  '%s' and channel <> 'wifi'  " %(i, yesterday)
        mUser.Q(sql)
        res = mUser.fetchall();
        for v in res:
            tmpLst.append( v[0] )

    tmpLst = {}.fromkeys(tmpLst).keys()
    print "channelList = " , tmpLst

    dictUid = {}
    dictInviteUid = {}
    dictDownloadUid = {}
    for var in tmpLst :   # initial dict , key = channel , value = []
        dictUid[ var ] =[]
        dictInviteUid[ var ] =[]
        dictDownloadUid[ var ] =[]

    
    mZhuan = mmysql(_options.db["db_zhuan"])

    deviceLog  = []
    sql = "select device_id from z_device_log  where  LEFT(ctime,10 ) = '%s' and channel = 'wifi' " %yesterday
    mZhuan.Q(sql)
    res1 = mZhuan.fetchall();

    if res1:
        tmpLst = []
        for v in res1:
            tmpLst.append( v[0] )
        deviceLog.extend( tmpLst )    # device_log : key = channel , value = device_id

    deviceLog ="( '" + "' , '".join(deviceLog)  + "' )"

    mZhuan.close()



    mUser = mmysql(_options.db["db_user"])

    wifiNum = 0
    lockNum = 0

    for i in range(0,10): # 获取每个渠道号的用户列表( 此处的渠道号为 红包锁屏的渠道为wifi的 红包wifi(注册或使用)的渠道号)
        
        sql = "select * from app_register_%d  where  uid  in (select uid from app_register_%d  where channel = 'wifi'  and LEFT(rtime,10) = '%s' order by uid  desc) and channel <> 'wifi' " %(i, i, yesterday)

        mUser.Q(sql)
        res = mUser.fetchall();

        for var in res :
            dictUid[ var[6] ].append(str(var[0]))   # value = uid

        sql = "select * from app_register_%d  where  device_id  in  %s  and channel <> 'wifi' and channel <> 'share'   and LEFT(rtime,10) = '%s'  " %(i, deviceLog, yesterday)
        mUser.Q(sql)
        res = mUser.fetchall();

        for var in res :
            dictUid[ var[6] ].append(str(var[0])) 


        sql = "select count(*) from app_register_%d  where appid = 1 and  LEFT(rtime,10) = '%s' order by uid  desc " %(i, yesterday)
        mUser.Q(sql)
        wifiNum += mUser.fetchall()[0][0];

        sql = "select count(*) from app_register_%d  where  uid  in (select uid from app_register_%d  where channel = 'wifi'  and LEFT(rtime,10) = '%s' order by uid  desc) and channel <> 'wifi' " %(i, i, yesterday)
        mUser.Q(sql)
        lockNum += mUser.fetchall()[0][0];

    mUser.close()

    ##############################################

    ######################################## db scorre
    for ii in range(0,10): # 遍历10 个库 
        dbscore = "db_score_%d" %ii
        mUser = mmysql(_options.db[dbscore])

        for i in range(0,10):  # 遍历10 个表
            
            for key ,var in dictUid.items(): #遍历渠道号
                if var :
                    
                    uidList ="(" + ",".join(var)  + ")"
                    sql = "select uid from z_score_log_%d  where time_stamp > '%d' and action_type = 2 and uid  in  %s" %(i, int(time.time()-86400), uidList) # 读取邀请过的用户
                                    
                    mUser.Q(sql)
                    res = mUser.fetchall();

                    if res:
                        tmpLst = []
                        for v in res:
                            tmpLst.append( v[0] )
                        dictInviteUid[ key ] .extend( tmpLst )

    #####
                    sql = "select uid from z_score_log_%d  where time_stamp > '%d' and action_type = 0 and uid  in  %s" %(i, int(time.time()-86400), uidList) # 读取下载过的用户
                                    
                    mUser.Q(sql)
                    res = mUser.fetchall();

                    if res:
                        tmpLst = []
                        for v in res:
                            tmpLst.append( v[0] )
                        dictDownloadUid[ key ] .extend( tmpLst )

    mWifi = mmysql(_options.db["db_wifi"])


    # yesterday = "2015-06-05"

    # print yesterday


    print 

    print "wifi register num" , wifiNum
    print  "lock register num" , lockNum


  # `lock_num` int(11) DEFAULT NULL COMMENT '昨日wifi渠道lock新增用户总数',
  # `invite_num` int(11) DEFAULT NULL COMMENT '昨日wifi渠道lock新增用户邀请总量',
  # `user_invite_num` int(11) DEFAULT NULL COMMENT '昨日wifi渠道lock新增用户中有邀请行为的用户数',
  # `download_num` int(11) DEFAULT NULL COMMENT '昨日wifi渠道lock新增用户下载广告的总量',
  # `user_download_num` int(11) DEFAULT NULL COMMENT ' 昨日wifi渠道lock新增用户有下载行为的用户数


    
    for key , val in dictInviteUid.items():
        lock_num = len( dictUid[key] )  
        if val: 

            invite_num = len(val)
            user_invite_num = len({}.fromkeys(val).keys() ) 

            num =  float(invite_num) /  lock_num* 100    #每百人邀请用户的个数
            rate = float( user_invite_num ) / lock_num     #有邀请行为的用户比例
            print "每百人邀请用户的个数", key, num
            print  "有邀请行为的用户比例", key,  rate
            print 

            sql = "update analytics_user_active set  lock_num = %d, invite_num = %d , user_invite_num = %d   where channel = '%s'  and cdate = '%s' " %( len( dictUid[key] ), len(val) ,  len({}.fromkeys(val).keys() ) , key , yesterday)
            mWifi.Q(sql)

    print 
    for key , val in dictDownloadUid.items():
        if val:        

            download_num = len(val)
            user_download_num = len({}.fromkeys(val).keys() ) 

            num =  float(download_num) /  lock_num* 100    #每百人xiazai ad的个数
            rate = float( user_download_num ) / lock_num     #有xiazai行为的用户比例

            print "每百人下载CPA个数", key,  num
            print  "有下载行为的用户比例", key, rate             
            
            sql = "update analytics_user_active set  download_num = %d, user_download_num = %d  where channel = '%s'  and cdate = '%s' " %(  len(val) ,  len({}.fromkeys(val).keys() ) , key , yesterday)
            print sql
            mWifi.Q(sql)


    mWifi.close()

if __name__ == '__main__':

    stateInviteAndDownload()
