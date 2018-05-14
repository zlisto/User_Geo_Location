import os
import sqlite3
from random import shuffle
from datetime import datetime
import time
import pymysql

#############################################################################################
####################### INPUTS ##############################################################

from twython import Twython
db_name='barcelona.db'
max_queries_per_user=16
# Test
with open('twitter_credentials.py') as f:
    lines = f.read().splitlines()

twitter = Twython(*lines)

# Working directory is where the sqlite database file is found!
working_directory="/Users/nicolas/Documents/MIT/RA/Code"


#############################################################################################
######################## BELOW SHOULD NOT NEED TO BE ALTERED ################################


#############################################################################################
######################## Tweet Data Class ###################################################

class TweetData:
    def __init__(
        self,
        conn
    ):
        self.cnx=conn
        self.c=self.cnx.cursor()
        return(None)
    def totranslate(self,string):
        tempstr=string.replace("'","^").encode('unicode-escape').decode('utf-8')
        return(tempstr)
    def create_tweet_tables(self):
        self.c.execute('CREATE TABLE IF NOT EXISTS user_profile (user_id BIGINT,' #user table needs some updates, to include current status id (should also be inserted into tweets)
               +'screen_name VARCHAR(40),'
                +'name VARCHAR(40),'
               +'created_at DATETIME,'
               +'description VARCHAR(200),'
                +'status_id BIGINT,'
               +'geo_enabled VARCHAR(5),'
               +'protected VARCHAR(5),'
                +'friends_count INT,'
                +'followers_count INT,'
                +'favourites_count INT,'
                +'statuses_count INT,'
                +'lang VARCHAR(3),'
                +'location VARCHAR(75),'
                +'verified VARCHAR(5),'
                +'profile_url VARCHAR(200),'
                +'default_image VARCHAR(5),'
                +'time_zone VARCHAR(45),'
                +'UTC_offset INT,'
               +'profile_pic_hash CHAR(16),'
               +'profile_pic_filename VARCHAR(40),'
               +'profile_banner_hash CHAR(16),'
               +'profile_banner_filename VARCHAR(40),'
                +'obtained DATE,'
                +'last_accessed DATE,'
                +'active VARCHAR(5),'
                +'PRIMARY KEY (user_id,screen_name)'
                +');')
        self.c.execute('CREATE INDEX IF NOT EXISTS sn_index ON user_profile(screen_name);')
    #             self.c.execute('CREATE INDEX name ON user_profile(name);')
        self.c.execute('CREATE TABLE IF NOT EXISTS tweet (tweet_id BIGINT PRIMARY KEY,'
               +'user_id BIGINT,'
                +'screen_name VARCHAR(40),'
               +'created_at DATETIME,'
               +'text VARCHAR(180),'
               +'geo_lat FLOAT,'
                +'geo_long FLOAT,'
                +'place_type VARCHAR(20),'
                +'place_name VARCHAR(40),'
                +'lang VARCHAR(3),'
                +'source VARCHAR(110),'
                +'retweet_count INT,'
                +'favorite_count INT,'
                +'retweet_status_id BIGINT,'
                +'reply_to_status_id BIGINT,'
                +'reply_to_user_id BIGINT,'
                +'reply_to_screen_name VARCHAR(40),'
               +'FOREIGN KEY (user_id,screen_name) REFERENCES user_profile (user_id,screen_name)'
    #                        +'FOREIGN KEY (reply_to_user_id,reply_to_screen_name) REFERENCES user_profile (user_id,screen_name),'
    #                        +'FOREIGN KEY (reply_to_status_id) REFERENCES tweet (tweet_id)
                +');')
        self.c.execute('CREATE INDEX IF NOT EXISTS sn_tweet_index ON tweet(screen_name);')
        self.c.execute('CREATE INDEX IF NOT EXISTS user_id_index ON tweet(user_id);')
        # self.c.execute('CREATE TABLE IF NOT EXISTS followers (ego_id BIGINT,'
        #        +'follower_id BIGINT,'
        #        +'FOREIGN KEY (ego_id) REFERENCES user (user_id),'
        #         +'FOREIGN KEY (follower_id) REFERENCES user (user_id),'
        #         +'PRIMARY KEY (ego_id,follower_id)'
        #         +');')
        self.c.execute('CREATE TABLE IF NOT EXISTS tweet_media (tweet_id BIGINT,'
               +'pic_hash CHAR(16),'
               +'pic_id BIGINT,'
               +'pic_source_user_id BIGINT,'
               +'pic_source_status_id BIGINT,'
               +'pic_filename VARCHAR(70),'
                +'url VARCHAR(80),'
                +'media_url VARCHAR(80),'
                +'display_url VARCHAR(80),'
                +'FOREIGN KEY (tweet_id) REFERENCES tweet (tweet_id),'
                +'PRIMARY KEY (tweet_id,pic_id)'
                +');')
        self.c.execute('CREATE TABLE IF NOT EXISTS tweet_hashtags (tweet_id BIGINT,'
               +'hashtag VARCHAR(50),'
                +'FOREIGN KEY (tweet_id) REFERENCES tweet (tweet_id),'
                +'PRIMARY KEY (tweet_id,hashtag)'
                +');')
        self.c.execute('CREATE TABLE IF NOT EXISTS tweet_usermentions (tweet_id BIGINT,'
               +'user_mention_id BIGINT,'
               +'user_mention_screen_name VARCHAR(40),'
               +'user_mention_name VARCHAR(40),'
                +'FOREIGN KEY (tweet_id) REFERENCES tweet (tweet_id),'
                +'PRIMARY KEY (tweet_id,user_mention_id)'
                +');')
        self.c.execute('CREATE TABLE IF NOT EXISTS tweet_url (tweet_id BIGINT,'
               +'url VARCHAR(100),'
               +'expanded_url VARCHAR(200),'
                +'FOREIGN KEY (tweet_id) REFERENCES tweet (tweet_id),'
                +'PRIMARY KEY (tweet_id,url)'
                +');')
        self.c.execute("CREATE TABLE IF NOT EXISTS collected (user_id BIGINT,"
                    +"date_collected DATE,"
                    +"first_tweet BIGINT,"
                    +"last_tweet BIGINT,"
                    +"FOREIGN KEY (user_id) REFERENCES user (user_id),"
                    +"FOREIGN KEY (first_tweet) REFERENCES tweet (tweet_id),"
                    +"FOREIGN KEY (last_tweet) REFERENCES tweet (tweet_id)"
                    +");")
    # def update_links(self,update_size=2000):
    #     count=0
    #     self.c.execute('SELECT count(1) from link;')
    #     result=c.fetchall()
    #     link_count=result[0][0]
    #     while count*update_size < link_count:
    #         self.c.execute("SELECT * FROM link LIMIT {0:d} OFFSET {1:d};".format(update_size,count*update_size))
    #         all_links=self.c.fetchall()
    #         count+=1
    #         qstr="INSERT OR IGNORE INTO followers (ego_id, follower_id) VALUES "
    #         if all_links[0][2] and not all_links[0][3]:
    #             qstr+=" ("+str(all_links[0][1])+","+str(all_links[0][0])+")"
    #         elif all_links[0][2]:
    #             qstr+="("+str(all_links[0][1])+","+str(all_links[0][0])+")"
    #             qstr+=",("+str(all_links[0][0])+","+str(all_links[0][1])+")"
    #         else:
    #             qstr+="("+str(all_links[0][0])+","+str(all_links[0][1])+")"
    #         for i in all_links[1:update_size]:
    #             if i[2]:
    #                 qstr+=",("+str(i[1])+","+str(i[0])+")"
    #             if i[3]:
    #                 qstr+=",("+str(i[0])+","+str(i[1])+")"
    #         qstr+=";"
    #         self.c.execute(qstr)
    def get_location_userids(self):
        self.c.execute("SELECT user_id FROM user WHERE Label = 1;")
        location_users=self.c.fetchall()
        location_users=[i[0] for i in location_users]
        return(location_users)
    def get_collected_userids(self):
        self.c.execute("SELECT user_id FROM collected;")
        collected_users=self.c.fetchall()
        collected_users=[i[0] for i in collected_users]
        return(collected_users)
    def insertUserProfiles(self,users,obtained_date,access_date): #Insert a collection of users accessed on a certain date.
        #First make string of values to insert.
        ca=[datetime.strptime(i['created_at'].replace('+0000','UTC'),'%a %b %d %H:%M:%S %Z %Y') for i in users]
        u=users
        while(len(u)>0):
            users=u[0:1000]
            u=u[1000:max(1000,len(u))]
            created_at=ca[0:1000]
            ca=ca[1000:max(1000,len(ca))]
            profilestring=""
            for i in range(len(users)):
                    user=users[i]
                    profilestring+='('
                    profilestring+= str(user['id'])+','
                    profilestring+= "'"+str(user['screen_name'])+"',"
                    profilestring+= "'"+self.totranslate(user_profile['name'])[0:40]+" ',"
                    profilestring+= "'"+created_at[i].strftime("%Y-%m-%d %H:%M:%S")+"',"
                    profilestring+= "'"+self.totranslate(user['description'])[0:200]+" ',"
                    if 'status' in user.keys():
                        profilestring+= str(user['status']['id'])+","
                    else:
                        profilestring+='NULL,'
                    profilestring+= "'"+str(user['geo_enabled'])+"',"
                    profilestring+= "'"+str(user['protected'])+"',"
                    profilestring+= str(user['friends_count'])+','
                    profilestring+= str(user['followers_count'])+','
                    profilestring+= str(user['favourites_count'])+','
                    profilestring+= str(user['statuses_count'])+','
                    if user['lang'] is None:
                        profilestring+= 'NULL,'
                    else:
                        profilestring+= "'"+str(user['lang'])[0:3]+"',"
                    if user['location'] is None:
                        profilestring+= 'NULL,'
                    else:
                        profilestring+= "'"+self.totranslate(user['location'])[0:75]+" ',"
                    profilestring+= "'"+str(user['verified'])+"',"
                    if user['url'] is None:
                        profilestring += 'NULL,'
                    else:
                        if user['entities']['url']['urls'][0]['expanded_url'] is None:
                            profilestring+="'"+str(user['entities']['url']['urls'][0]['url'].replace("'","''"))+"',"
                        else:
                            profilestring+= "'"+str(user['entities']['url']['urls'][0]['expanded_url'].replace("'","''"))+"',"
                    profilestring+= "'"+str(user['default_profile_image'])+"',"
                    if user['time_zone'] is None:
                        profilestring+='NULL,NULL,'
                    else:
                        profilestring+= "'"+self.totranslate(user['time_zone'])+"',"
                        profilestring+= str(user['utc_offset'])+','
                    profilestring+="NULL,NULL,NULL,NULL,"
                    profilestring+= "'"+obtained_date +"',"
                    profilestring+= "'"+access_date +"',"
                    profilestring+= "'True'"#No comma here
                    if i==len(users)-1:
                        profilestring+=')'
                    else:
                        profilestring+= '),\n'
            if profilestring !="":
                self.c.execute('INSERT OR REPLACE INTO user_profile (user_id,'
                       +'screen_name,'
                        +'name,'
                       +'created_at,'
                       +'description,'
                        +'status_id,'
                       +'geo_enabled,'
                       +'protected,'
                        +'friends_count,'
                        +'followers_count,'
                        +'favourites_count,'
                        +'statuses_count,'
                        +'lang,'
                        +'location,'
                        +'verified,'
                        +'profile_url,'
                        +'default_image,'
                        +'time_zone,'
                        +'UTC_offset,'
                        +'profile_pic_hash,'
                        +'profile_pic_filename,'
                        +'profile_banner_hash,'
                        +'profile_banner_filename,'
                        +'obtained,'
                        +'last_accessed,'
                        +'active'
                        +')\n'
                        +'VALUES\n'
                        +profilestring
                        +'\n'
                        +';')
    def insertUsersfromTweets(self,tweets,access_date): #Insert a collection of users accessed on a certain date.
        #First make string of values to insert.
        od=[datetime.strptime(i['created_at'].replace('+0000','UTC'),'%a %b %d %H:%M:%S %Z %Y') for i in tweets]
        u=[i['user'] for i in tweets]
        ca=[datetime.strptime(i['created_at'].replace('+0000','UTC'),'%a %b %d %H:%M:%S %Z %Y') for i in u]
        while(len(u)>0):
            users=u[0:1000]
            u=u[1000:max(1000,len(u))]
            created_at=ca[0:1000]
            ca=ca[1000:max(1000,len(ca))]
            obtained_date=od[0:1000]
            od=od[1000:max(1000,len(od))]
            profilestring=""
            for i in range(len(users)):
                user=users[i]
                profilestring+='('
                profilestring+= str(user['id'])+','
                profilestring+= "'"+str(user['screen_name'])+"',"
                profilestring+= "'"+self.totranslate(user['name'])[0:40]+" ',"
                profilestring+= "'"+created_at[i].strftime("%Y-%m-%d %H:%M:%S")+"',"
                profilestring+= "'"+self.totranslate(user['description'])[0:200]+" ',"
                if 'status' in user.keys():
                    profilestring+= str(user['status']['id'])+","
                else:
                    profilestring+='NULL,'
                profilestring+= "'"+str(user['geo_enabled'])+"',"
                profilestring+= "'"+str(user['protected'])+"',"
                profilestring+= str(user['friends_count'])+','
                profilestring+= str(user['followers_count'])+','
                profilestring+= str(user['favourites_count'])+','
                profilestring+= str(user['statuses_count'])+','
                if user['lang'] is None:
                    profilestring+= 'NULL,'
                else:
                    profilestring+= "'"+str(user['lang'])[0:3]+"',"
                if user['location'] is None:
                    profilestring+= 'NULL,'
                else:
                    profilestring+= "'"+self.totranslate(user['location'])[0:75]+" ',"
                profilestring+= "'"+str(user['verified'])+"',"
                if user['url'] is None:
                    profilestring += 'NULL,'
                else:
                    if user['entities']['url']['urls'][0]['expanded_url'] is None:
                        profilestring+="'"+str(user['entities']['url']['urls'][0]['url'].replace("'","''"))+"',"
                    else:
                        profilestring+= "'"+str(user['entities']['url']['urls'][0]['expanded_url'].replace("'","''"))+"',"
                profilestring+= "'"+str(user['default_profile_image'])+"',"
                if user['time_zone'] is None:
                    profilestring+='NULL,NULL,'
                else:
                    profilestring+= "'"+self.totranslate(user['time_zone'])+"',"
                    profilestring+= str(user['utc_offset'])+','
                profilestring+="NULL,NULL,NULL,NULL,"
                profilestring+= "'"+obtained_date[i].strftime('%Y-%m-%d') +"',"
                profilestring+= "'"+access_date +"',"
                profilestring+= "'True'"#No comma here
                if i==len(users)-1:
                    profilestring+=')'
                else:
                    profilestring+= '),\n'
            if profilestring !="":
                self.c.execute('INSERT OR REPLACE INTO user_profile (user_id,'
                       +'screen_name,'
                        +'name,'
                       +'created_at,'
                       +'description,'
                       +'status_id,'
                       +'geo_enabled,'
                       +'protected,'
                        +'friends_count,'
                        +'followers_count,'
                        +'favourites_count,'
                        +'statuses_count,'
                        +'lang,'
                        +'location,'
                        +'verified,'
                        +'profile_url,'
                        +'default_image,'
                        +'time_zone,'
                        +'UTC_offset,'
                        +'profile_pic_hash,'
                        +'profile_pic_filename,'
                        +'profile_banner_hash,'
                        +'profile_banner_filename,'
                        +'obtained,'
                        +'last_accessed,'
                        +'active'
                        +')\n'
                        +'VALUES\n'
                        +profilestring
                        +'\n'
                        +';')
    def insertTweets(self,tweets): #put the tweet in the tweet table, update the tweet media, tweet hashtag, tweet url
        #First make string of values to insert.
        ca=[datetime.strptime(i['created_at'].replace('+0000','UTC'),'%a %b %d %H:%M:%S %Z %Y') for i in tweets]
        t=tweets
        while(len(t)>0):
            tweets=t[0:1000]
            t=t[1000:max(1000,len(t))]
            created_at=ca[0:1000]
            ca=ca[1000:max(1000,len(ca))]
            tweetstring=""
            htstring=""
            umstring=""
            urlstring=""
            vals=""
            for i in range(len(tweets)):
                tweet=tweets[i]
                tweetstring+='('
                tweetstring+= str(tweet['id'])+','
                if 'user' in tweet.keys():
                    tweetstring+= str(tweet['user']['id'])+","
                    tweetstring+= "'"+tweet['user']['screen_name']+"',"
                else:
                    tweetstring+='NULL,NULL,'
                tweetstring+= "'"+created_at[i].strftime("%Y-%m-%d %H:%M:%S")+"',"
                tweetstring+= "'"+self.totranslate(tweet['text'])[0:180]+" ',"
                if tweet['geo'] is None:
                    tweetstring+='NULL,NULL,'
                else:
                    tweetstring+=str(tweet['geo']['coordinates'][0])+','+str(tweet['geo']['coordinates'][1])+','
                if tweet['place'] is None:
                    tweetstring+='NULL,NULL,'
                else:
                    tweetstring+= "'"+tweet['place']['place_type']+"',"
                    tweetstring+= "'"+self.totranslate(tweet['place']['name'])[0:40]+" ',"
                tweetstring+= "'"+str(tweet['lang'])[0:3]+"',"
                tweetstring+="'"+tweet['source'].replace("'","''")[0:109]+" ',"
                if 'retweet_count' in tweet.keys():
                    tweetstring+= str(tweet['retweet_count'])+","
                else:
                    tweetstring+= "NULL,"
                if 'favorite_count' in tweet.keys():
                    tweetstring+= str(tweet['favorite_count'])+","
                else:
                    tweetstring+= "NULL,"
                if 'retweeted_status' in tweet.keys():
                    tweetstring+=str(tweet['retweeted_status']['id'])+','
                else:
                    tweetstring+='NULL,'
                if tweet['in_reply_to_status_id'] is None:
                    tweetstring+='NULL,NULL,NULL'
                else:
                    tweetstring+=str(tweet['in_reply_to_status_id'])+','+str(tweet['in_reply_to_user_id'])+",'"+tweet['in_reply_to_screen_name']+"'"
                if i==len(tweets)-1:
                    tweetstring+=')'
                else:
                    tweetstring+= '),\n'
                for j in tweet['entities']['hashtags']:
                    if htstring=="":
                        htstring+='('
                    else:
                        htstring+=',\n('
                    htstring+=str(tweet['id'])+','
                    htstring+="'"+j['text'][0:180]+"')"
                for j in tweet['entities']['urls']:
                    if urlstring=="":
                        urlstring+='('
                    else:
                        urlstring+=',\n('
                    urlstring+=str(tweet['id'])+','
                    urlstring+="'"+j['url'].replace("'","''")+"',"
                    if 'expanded_url' in j.keys():
                        urlstring+="'"+j['expanded_url'].replace("'","''")+"')"
                    else:
                        urlstring+="NULL)"                                
                for j in tweet['entities']['user_mentions']:
                    if umstring=="":
                        umstring+='('
                    else:
                        umstring+=',\n('
                    umstring+=str(tweet['id'])+','
                    umstring+=str(j['id'])+","
                    umstring+="'"+j['screen_name']+"',"
                    umstring+="'"+self.totranslate(j['name'])[0:40]+" ')"
                if 'media' in tweet['entities'].keys():
                    if 'extended_entities' in tweet.keys():
                        txt='extended_entities'
                    else:
                        txt='entities'
                    for k in range(len(tweet[txt]['media'])):
                        if vals=="":
                            vals+='('
                        else:
                            vals+=',\n('
                        j=tweet[txt]['media'][k]
                        vals+=str(tweet['id'])+","
                        vals+="NULL,"
                        vals+=str(j['id'])+','
                        if 'source_status_id' in j.keys():
                            vals+=str(j['source_user_id'])+","
                            vals+=str(j['source_status_id'])+","
                        else:
                            vals+="NULL,NULL,"
                        vals+="NULL,"
                        vals+="'"+j['url']+"',"
                        vals+="'"+j['media_url'][0:79]+" ',"
                        vals+="'"+j['display_url']+"')"
            if tweetstring !="":
                self.c.execute('INSERT OR REPLACE INTO tweet (tweet_id,'
                       +'user_id,'
                        +'screen_name,'
                       +'created_at,'
                       +'text,'
                       +'geo_lat,'
                        +'geo_long,'
                        +'place_type,'
                        +'place_name,'
                        +'lang,'
                        +'source,'
                        +'retweet_count,'
                        +'favorite_count,'
                        +'retweet_status_id,'
                        +'reply_to_status_id,'
                        +'reply_to_user_id,' 
                        +'reply_to_screen_name'#No comma here!
                        +')\n'
                        +'VALUES\n'
                        +tweetstring
                        +';')
            if htstring !="":
                self.c.execute('INSERT OR IGNORE INTO tweet_hashtags (tweet_id,'
                       +'hashtag'
                        +')\n'
                        +'VALUES\n'
                        +htstring
                        +';')
            if umstring !="":
                self.c.execute('INSERT OR IGNORE INTO tweet_usermentions (tweet_id,'
                       +'user_mention_id,'
                       +'user_mention_screen_name,'
                       +'user_mention_name'
                        +')\n'
                        +'VALUES\n'
                        +umstring
                        +';')
            if urlstring !="":
                self.c.execute('INSERT OR IGNORE INTO tweet_url (tweet_id,'
                       +'url,'
                       +'expanded_url'
                        +')\n'
                        +'VALUES\n'
                        +urlstring
                        +';')
            if vals !="":
                self.c.execute('INSERT OR REPLACE INTO tweet_media (tweet_id,'
                       +'pic_hash,'
                       +'pic_id,'
                       +'pic_source_user_id,'
                       +'pic_source_status_id,'
                       +'pic_filename,'
                        +'url,'
                        +'media_url,'
                        +'display_url'
                        +')\n'
                        +'VALUES\n'
                        +vals
                        +';')
    def insertStatuses(self,users): #put the tweet in the tweet table, update the tweet media, tweet hashtag, tweet url
        #First make string of values to insert.  #Retweets in Statuses don't have original users attached.
        t=[i['status'] for i in users if 'status' in i.keys()]
        ul=[{'id':i['id'],'screen_name':i['screen_name']} for i in users if 'status' in i.keys()]
        ca=[datetime.strptime(i['created_at'].replace('+0000','UTC'),'%a %b %d %H:%M:%S %Z %Y') for i in t]
        while(len(t)>0):
            tweets=t[0:1000]
            t=t[1000:max(1000,len(t))]
            created_at=ca[0:1000]
            ca=ca[1000:max(1000,len(ca))]
            userlist=ul[0:1000]
            ul=ul[1000:max(1000,len(ul))]
            tweetstring=""
            htstring=""
            umstring=""
            urlstring=""
            vals=""
            for i in range(len(tweets)):
                tweet=tweets[i]
                tweetstring+='('
                tweetstring+= str(tweet['id'])+','
                tweetstring+= str(userlist[i]['id'])+","
                tweetstring+= "'"+userlist[i]['screen_name']+"',"
                tweetstring+= "'"+created_at[i].strftime("%Y-%m-%d %H:%M:%S")+"',"
                tweetstring+= "'"+self.totranslate(tweet['text'])[0:180]+" ',"
                if tweet['geo'] is None:
                    tweetstring+='NULL,NULL,'
                else:
                    tweetstring+=str(tweet['geo']['coordinates'][0])+','+str(tweet['geo']['coordinates'][1])+','
                if tweet['place'] is None:
                    tweetstring+='NULL,NULL,'
                else:
                    tweetstring+= "'"+tweet['place']['place_type']+"',"
                    tweetstring+= "'"+self.totranslate(tweet['place']['name'])+" ',"
                tweetstring+= "'"+str(tweet['lang'])[0:3]+"',"
                tweetstring+= "'"+tweet['source'].replace("'","''")[0:109]+" ',"
                if 'retweet_count' in tweet.keys():
                    tweetstring+=str(tweet['retweet_count'])+","
                else:
                    tweetstring+="NULL,"
                if 'favorite_count' in tweet.keys():
                    tweetstring+=str(tweet['favorite_count'])+","
                else:
                    tweetstring+="NULL,"
                if 'retweeted_status' in tweet.keys():
                    tweetstring+=str(tweet['retweeted_status']['id'])+','
                else:
                    tweetstring+='NULL,'
                if tweet['in_reply_to_status_id'] is None:
                    tweetstring+='NULL,NULL,NULL'
                else:
                    tweetstring+=str(tweet['in_reply_to_status_id'])+','+str(tweet['in_reply_to_user_id'])+",'"+tweet['in_reply_to_screen_name']+"'"
                if i==len(tweets)-1:
                    tweetstring+=')'
                else:
                    tweetstring+= '),\n'
                for j in tweet['entities']['hashtags']:
                    if htstring=="":
                        htstring+='('
                    else:
                        htstring+=',\n('
                    htstring+=str(tweet['id'])+','
                    htstring+="'"+j['text'][0:160]+"')"
                for j in tweet['entities']['urls']:
                    if urlstring=="":
                        urlstring+='('
                    else:
                        urlstring+=',\n('
                    urlstring+=str(tweet['id'])+','
                    urlstring+="'"+j['url'].replace("'","''")+"',"
                    if 'expanded_url' in j.keys():
                        urlstring+="'"+j['expanded_url'].replace("'","''")+"')"
                    else:
                        urlstring+="NULL)"                                
                for j in tweet['entities']['user_mentions']:
                    if umstring=="":
                        umstring+='('
                    else:
                        umstring+=',\n('
                    umstring+=str(tweet['id'])+','
                    umstring+=str(j['id'])+","
                    umstring+="'"+j['screen_name']+"',"
                    umstring+="'"+self.totranslate(j['name'])[0:40]+" ')"
                if 'media' in tweet['entities'].keys():
                    if 'extended_entities' in tweet.keys():
                        txt='extended_entities'
                    else:
                        txt='entities'
                    for k in range(len(tweet[txt]['media'])):
                        if vals=="":
                            vals+='('
                        else:
                            vals+=',\n('
                        j=tweet[txt]['media'][k]
                        vals+=str(tweet['id'])+","
                        vals+="NULL,"
                        vals+=str(j['id'])+','
                        if 'source_status_id' in j.keys():
                            vals+=str(j['source_user_id'])+","
                            vals+=str(j['source_status_id'])+","
                        else:
                            vals+="NULL,NULL,"
                        vals+="NULL,"
                        vals+="'"+j['url']+"',"
                        vals+="'"+j['media_url'][0:79]+" ',"
                        vals+="'"+j['display_url']+"')"
            if tweetstring !="":
                self.c.execute('INSERT OR REPLACE INTO tweet (tweet_id,'
                       +'user_id,'
                        +'screen_name,'
                       +'created_at,'
                       +'text,'
                       +'geo_lat,'
                        +'geo_long,'
                        +'place_type,'
                        +'place_name,'
                        +'lang,'
                         +'source,'
                        +'retweet_count,'
                        +'favorite_count,'
                        +'retweet_status_id,'
                        +'reply_to_status_id,'
                        +'reply_to_user_id,' 
                        +'reply_to_screen_name'#No comma here!
                        +')\n'
                        +'VALUES\n'
                        +tweetstring
                         +';')
            if htstring !="":
                self.c.execute('INSERT OR IGNORE INTO tweet_hashtags (tweet_id,'
                       +'hashtag'
                        +')\n'
                        +'VALUES\n'
                        +htstring
                        +';')
            if umstring !="":
                self.c.execute('INSERT OR IGNORE INTO tweet_usermentions (tweet_id,'
                       +'user_mention_id,'
                       +'user_mention_screen_name,'
                       +'user_mention_name'
                        +')\n'
                        +'VALUES\n'
                        +umstring
                        +';')
            if urlstring !="":
                self.c.execute('INSERT OR IGNORE INTO tweet_url (tweet_id,'
                       +'url,'
                       +'expanded_url'
                        +')\n'
                        +'VALUES\n'
                        +urlstring
                        +';')
            if vals !="":
                self.c.execute('INSERT OR REPLACE INTO tweet_media (tweet_id,'
                       +'pic_hash,'
                       +'pic_id,'
                       +'pic_source_user_id,'
                       +'pic_source_status_id,'
                       +'pic_filename,'
                        +'url,'
                        +'media_url,'
                        +'display_url'
                        +')\n'
                        +'VALUES\n'
                        +vals
                        +';')
    def allFromTweets(self,tweets,access_date):
        rts=[i['retweeted_status'] for i in tweets if 'retweeted_status' in i.keys()]
        y=[i for i in tweets if i['id'] not in [j['id'] for j in rts]]
        z=rts+y
        users=[i['user'] for i in z]
        self.insertUsersfromTweets(z,access_date)
        self.cnx.commit()
        self.insertStatuses(users)
        self.cnx.commit()
        self.insertTweets(rts)
        self.cnx.commit()
        self.insertTweets(y)
        self.cnx.commit()
        return(None)
    def insert_user_collected(self,user_id,date_collected,first_tweet=None,last_tweet=None):
        if (first_tweet is None) or (last_tweet is None):
            self.c.execute("INSERT INTO collected (user_id,date_collected) VALUES "
                    +"("+str(user_id)+",'"+date_collected+"');")
        else:
            self.c.execute("INSERT INTO collected (user_id,date_collected,first_tweet,last_tweet) VALUES "
                    +"("+str(user_id)+",'"+date_collected+"',"+str(first_tweet)+","+str(last_tweet)+");")

##########################################################################################################
############################### EXECUTION CODE ###########################################################



os.chdir(working_directory)


conn=sqlite3.connect(db_name)
tweet_data=TweetData(conn)

tweet_data.create_tweet_tables()
# tweet_data.update_links()


location_users=tweet_data.get_location_userids()
collected_users=tweet_data.get_collected_userids()


remaining_users=list(set(location_users)-set(collected_users))
shuffle(remaining_users)

print(len(remaining_users))
start_time=datetime.now()
query_start=datetime.now()
while (len(remaining_users)>0):
    tweets=[]
    time.sleep(max(0,(1-(datetime.now()-query_start).total_seconds())))
    query_start=datetime.now()
    date_now=datetime.now().strftime("%Y-%m-%d")
    uid=remaining_users.pop(0)
    collect=True
    try:
        user_profile=twitter.show_user(user_id=uid)
        collect=True
    except:
        time.sleep(1)
        try:
            user_profile=twitter.show_user(user_id=uid)
            collect=True
        except:
            collect=False
            print("Collect failed for user "+str(uid))
    if collect:
        tweet_data.insertUserProfiles([user_profile],date_now,date_now)
        conn.commit()
        if (user_profile['statuses_count']>0) and not (user_profile['protected']):
            try:
                temp_tweets=twitter.get_user_timeline(user_id=uid,count=200)
                query_count=1
            except:
                time.sleep(1)
                try:
                    temp_tweets=twitter.get_user_timeline(user_id=uid,count=200)
                    query_count=1
                except:
                    temp_tweets=[]
                    query_count=0
            tweets+=temp_tweets
            tweet_data.allFromTweets(temp_tweets,date_now)
            while (len(temp_tweets)>0) and (query_count < max_queries_per_user):
                conn.commit()
                # print("   "+str(max(0,(1-(datetime.now()-query_start).total_seconds()))))
                time.sleep(max(0,(1-(datetime.now()-query_start).total_seconds())))
                query_start=datetime.now()
                # print("   "+str(len(tweets)))
                m_id=min([i['id'] for i in temp_tweets])-1
                try:
                    temp_tweets=twitter.get_user_timeline(user_id=uid,count=200,max_id=m_id)
                except:
                    time.sleep(1)
                    try:
                        temp_tweets=twitter.get_user_timeline(user_id=uid,count=200,max_id=m_id)
                    except:
                        temp_tweets=[]
                        print("Error getting follow-on tweets user {0:d} (@{1:s})".format(uid,user_profile['screen_name']))
                tweets+=temp_tweets
                tweet_data.allFromTweets(temp_tweets,date_now)
                query_count+=1
    if len(tweets)>0:
        first_tweet=min([i['id'] for i in tweets])
        last_tweet=max([i['id'] for i in tweets])
        tweet_data.insert_user_collected(uid,date_now,first_tweet,last_tweet)
    else:
        tweet_data.insert_user_collected(uid,date_now)
    print("Inserted "+str(len(tweets))+" tweets from user "+str(uid)+".")
    if len(remaining_users) % 10 == 0:
    	print("\n{0:d} Users remaining.\n".format(len(remaining_users)))

conn.close()




