from datetime import datetime,timedelta
from numpy import cos, pi, log, exp
from random import sample, random, shuffle
import networkx as nx
import time
import sys
import sqlite3
from typing import Callable
from collections import namedtuple
from gmplot import gmplot
from matplotlib import pyplot as plt
import os
from twython import Twython
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import os
import sys
from sklearn.linear_model import LogisticRegression
from numpy import cos,pi,log,exp
from gmplot import gmplot
import sqlite3
from datetime import datetime,timedelta
import time
from random import sample,random


################################################################################
##################### USER LOCATION DATA CLASS##################################

class UserData:
    """
    Class to manage user data connection, execute expand and classify steps.
    """
    def __init__(self,
                 conn: sqlite3.Connection,
                 loclatlong: tuple,
                 location_name: str="",
                 test_profile: str="zlisto"):
        """

        :param conn: sqlite3 connection for storing tweets on disk
        :param loclatlong: latitude and longitude of location
        :param location_name: name of location
        :param test_profile: UNKNOWN AND UNUSED
        """
        self.loclatlong = loclatlong
        self.cnx = conn
        self.c = self.cnx.cursor()
        self.location_name = location_name

    @property
    def user_count(self):
        return self.c.execute("SELECT COUNT(1) FROM user;").fetchall()[0][0]

    @staticmethod
    def lldist(point1,
               point2):
        """
        :param point1: lat/lon of point 1
        :param point2: lat/lon of point 2
        :return: distance in miles between the points
        """
        average_lat = (point1[0] + point2[0]) * 1. / 2
        lat_miles = 69
        long_miles = cos(average_lat * pi * 1. / 180) * 69.172
        lat_diff = (point1[0] - point2[0]) * lat_miles
        max_long = max(point1[1], point2[1])
        min_long = min(point1[1], point2[1])
        long_diff = min(max_long - min_long, 180 - max_long + min_long) * long_miles
        dist = pow(pow(lat_diff, 2) + pow(long_diff, 2), 0.5)
        return dist

    # TODO: What does this take in? (JIM)
    def create_tables(self, features_function_types):
        """

        :param features_function_types:
        :return:
        """
        self.c.execute("CREATE TABLE IF NOT EXISTS machine_learning_sets (set_no INT PRIMARY KEY,ML_set CHAR(3));")
        self.c.execute("INSERT OR IGNORE INTO machine_learning_sets VALUES (1,'TRN'),(2,'VAL'),(3,'TST');")
        self.c.execute("CREATE TABLE IF NOT EXISTS collections "
                       + "("
                       + "collection_no INT PRIMARY KEY,"
                       + "start_collect DATETIME,"
                       + "end_optimization DATETIME,"
                       + "pos_label_count INT,"
                       + "neg_label_count INT"
                       + ");"
                       )
        self.c.execute("CREATE TABLE IF NOT EXISTS user "
                       + "("
                       + "user_id BIGINT PRIMARY KEY,"
                       + "friends_count INT,"
                       + "followers_count INT,"
                       + "protected BOOL,"
                       + "verified BOOL,"
                       + "geo_enabled BOOL,"
                       + "statuses_count INT,"
                       + "tweet_rate FLOAT,"
                       + "collection_no INT,"
                       + "queried_friends CHAR(1),"
                       + "queried_followers CHAR(1),"
                       + "friend_query_page BIGINT,"
                       + "follower_query_page BIGINT,"
                       + "Label BOOL,"
                       + "Prob FLOAT,"
                       + "psi_local FLOAT,"
                       + "phi_local FLOAT,"
                       + "A00_contrib FLOAT,"
                       + "Res FLOAT,"
                       + "Optim BOOL,"
                       + "ML_set CHAR(3) REFERENCES machine_learning_sets (ML_set),"
                       + "FOREIGN KEY (collection_no) REFERENCES collections (collection_no)"
                       + ");")
        self.c.execute("CREATE TABLE IF NOT EXISTS link "
                       + "("
                       + "left_user_id BIGINT,"
                       + "right_user_id BIGINT,"
                       + "left_follows_right BOOL,"
                       + "right_follows_left BOOL,"
                       + "PRIMARY KEY (left_user_id,right_user_id),"
                       + "FOREIGN KEY (left_user_id) REFERENCES user (user_id),"
                       + "FOREIGN KEY (right_user_id) REFERENCES user (user_id)"
                       + ");"
                       )
        self.c.execute("CREATE TABLE IF NOT EXISTS geo_user ("
                       + "user_id BIGINT PRIMARY KEY,"
                       + "geo_tweet TINYINT,"
                       + "tweet_id BIGINT,"
                       + "lat FLOAT,"
                       + "lon FLOAT,"
                       + "dist FLOAT,"
                       + "name VARCHAR(40),"
                       + "screen_name VARCHAR(15),"
                       + "location VARCHAR(50),"
                       + "FOREIGN KEY (user_id) REFERENCES user(user_id)"
                       + ");")
        self.cnx.commit()
        # This next section creates the feature table--change data types if using other than binary.  In SQLite it should not matter.
        create_feature_table_str = "CREATE TABLE IF NOT EXISTS features (user_id BIGINT PRIMARY KEY,X_0 " + \
                                   features_function_types[0]
        for i in range(1, len(features_function_types)):
            create_feature_table_str += ",X_" + str(i) + " " + features_function_types[i]
        create_feature_table_str += ", FOREIGN KEY (user_id) REFERENCES user (user_id));"
        self.c.execute(create_feature_table_str)
        self.c.execute("CREATE TABLE IF NOT EXISTS collection_queries "
                       + "("
                       + "collection_no INT,"
                       + "user_id BIGINT,"
                       + "query VARCHAR(8),"
                       + "FOREIGN KEY (user_id) REFERENCES user (user_id),"
                       + "FOREIGN KEY (collection_no) REFERENCES collections (collection_no)"
                       + ");"
                       )
        self.cnx.commit()

    def add_users(self,
                  profiles,
                  feature_func,
                  collection_no=0,
                  label=0):
        """

        :param profiles:
        :param feature_func:
        :param collection_no:
        :param label:
        :return:
        """
        while len(profiles) > 0:
            us_1000 = profiles[0:1000]
            user_string = ""
            feature_string = ""
            user_id_string = "("
            for i in range(len(us_1000)):
                u = us_1000[i]
                user_id_string += str(u['id'])
                user_string += '('
                user_string += str(u['id']) + ","
                user_string += str(u["friends_count"]) + ","
                user_string += str(u["followers_count"]) + ","
                user_string += ('1' if u['protected'] else '0') + ","
                user_string += ('1' if u['verified'] else '0') + ","
                user_string += ('1' if u['geo_enabled'] else '0') + ","
                user_string += str(u['statuses_count']) + ","
                user_string += str(u['statuses_count'] * 60 * 60 * 24. / (
                datetime.now() - datetime.strptime(u['created_at'],
                                                   "%a %b %d %H:%M:%S +0000 %Y")).total_seconds()) + ","
                user_string += str(collection_no) + ","
                user_string += "'N',"  # queried_friends
                user_string += "'N',"  # queried_followers
                user_string += "NULL,"  # friend_query_page
                user_string += "NULL,"  # follower_query_page
                user_string += "{0:d},".format(label)  # Label
                user_string += "0.5,"  # Prob
                user_string += "NULL,"  # psi_local
                user_string += "NULL,"  # phi_local
                user_string += "NULL,"  # a00_contrib
                user_string += "NULL,"  # Res
                user_string += "1,"  # Optim
                r = random()
                if r < 0.5:
                    user_string += "'TRN'"
                elif r < 0.7:
                    user_string += "'VAL'"
                else:
                    user_string += "'TST'"
                user_string += ")"
                feature_string += '(' + str(u['id'])
                features = feature_func(u)
                for j in range(len(features)):
                    feature_string += "," + str(features[j])
                feature_string += ')'
                if i < (len(us_1000) - 1):
                    user_string += ","
                    feature_string += ","
                    user_id_string += ","
            user_id_string += ")"
            self.c.execute("INSERT OR IGNORE INTO user "
                           + "(user_id,"
                           + "friends_count,"
                           + "followers_count,"
                           + "protected,"
                           + "verified,"
                           + "geo_enabled,"
                           + "statuses_count,"
                           + "tweet_rate,"
                           + "collection_no,"
                           + "queried_friends,"
                           + "queried_followers,"
                           + "friend_query_page,"
                           + "follower_query_page,"
                           + "Label,"
                           + "Prob,"
                           + "psi_local,"
                           + "phi_local,"
                           + "a00_contrib,"
                           + "Res,"
                           + "Optim,"
                           + "ML_set"
                           + ") VALUES " + user_string + ";")
            self.c.execute("UPDATE user SET Optim=1 WHERE user_id IN " + user_id_string + ";")
            self.c.execute("INSERT OR IGNORE INTO features VALUES " + feature_string + ";")
            self.cnx.commit()
            profiles = profiles[1000:len(profiles)]

    def add_geo_user(self,
                     userid,
                     twython_obj):
        success = 0
        collected_timeline = True
        try:
            try:
                x = twython_obj.get_user_timeline(user_id=userid, count=200)
            except:
                time.sleep(1)
                try:
                    x = twython_obj.get_user_timeline(user_id=userid, count=200)
                except:
                    print("Error getting geo user " + str(userid) + " timeline.")
                    x = []
                    collected_timeline = False
            if collected_timeline:
                gt = [j for j in x if j['geo'] is not None]
                if len(gt) > 0:
                    distances = [
                        (
                            j['id'],
                            self.lldist(j['geo']['coordinates'], self.loclatlong)
                        )
                        for j in gt
                    ]
                    distances.sort(key=lambda xxx: xxx[1], reverse=False)
                    geo_tweet = [j for j in gt if j['id'] == distances[0][0]][0]
                    self.c.execute(
                        "INSERT OR IGNORE INTO geo_user (user_id, geo_tweet,tweet_id,lat,lon,dist,name,screen_name,location) VALUES "
                        + "("
                        + str(geo_tweet['user']['id']) + ","
                        + "1,"
                        + str(geo_tweet['id']) + ","
                        + str(geo_tweet['geo']['coordinates'][0]) + ","
                        + str(geo_tweet['geo']['coordinates'][1]) + ","
                        + str(distances[0][1]) + ","
                        + "'" + geo_tweet['user']['name'].replace("\n", "<br/>").replace('"', '^^').replace("'",
                                                                                                            "^").encode(
                            'ascii', 'xmlcharrefreplace').decode('ascii') + "', "
                        + "'" + geo_tweet['user']['screen_name'] + "', "
                        + "'" + geo_tweet['user']['location'].replace("\n", "<br/>").replace('"', '^^').replace("'",
                                                                                                                "^").encode(
                            'ascii', 'xmlcharrefreplace').decode('ascii') + "' "
                        + ");"
                        )
                    self.cnx.commit()
                    success = 1
                else:
                    self.c.execute("INSERT OR IGNORE INTO geo_user VALUES "
                                   + "("
                                   + str(userid) + ","
                                   + "0,NULL,NULL,NULL,NULL,NULL,NULL,NULL"
                                   + ")"
                                   + ";"
                                   )
                    self.cnx.commit()
        except:
            print("Geo Collect Error: " + str(userid))
        return success

    def add_links(self, ego_id, profiles, relationship_type):
        self.c.execute(
            "SELECT left_user_id,left_follows_right,right_follows_left FROM link WHERE right_user_id={0:d};".format(
                ego_id))
        left_user_ids = self.c.fetchall()

        self.c.execute(
            "SELECT right_user_id,right_follows_left,left_follows_right FROM link WHERE left_user_id={0:d};".format(
                ego_id))
        right_user_ids = self.c.fetchall()

        existing_relationships = {i[0]: {'follower_of_ego': i[1], 'friend_of_ego': i[2]} for i in
                                  left_user_ids + right_user_ids}
        link_string = ""
        for i in range(len(profiles)):
            u = profiles[i]
            if relationship_type == 'friend':
                is_friend = 1
                if u['id'] in existing_relationships:
                    is_follower = existing_relationships[u['id']]['follower_of_ego']
                else:
                    is_follower = 0
            else:
                if u['id'] in existing_relationships:
                    is_friend = existing_relationships[u['id']]['friend_of_ego']
                else:
                    is_friend = 0
                is_follower = 1
            link_string += '('
            if u['id'] < ego_id:
                link_string += '{0:d},{1:d},{2:d},{3:d}'.format(u['id'], ego_id, is_follower, is_friend)
            else:
                link_string += '{0:d},{1:d},{2:d},{3:d}'.format(ego_id, u['id'], is_friend, is_follower)
            link_string += ')'
            if i < (len(profiles) - 1):
                link_string += ","

        self.c.execute(
            "INSERT OR REPLACE INTO link (left_user_id,right_user_id,left_follows_right,right_follows_left) VALUES " + link_string
            + ";"
        )
        self.cnx.commit()

    def add_collection_start(self):
        self.c.execute("SELECT collection_no from collections ORDER BY collection_no DESC LIMIT 1;")
        result = self.c.fetchall()

        if len(result) > 0:
            collection_no = result[0][0] + 1
        else:
            collection_no = 0

        col_str = "INSERT INTO collections VALUES "
        col_str += "("
        col_str += "{0:d},".format(collection_no)  # number
        col_str += "'{0:s}',".format(datetime.now().isoformat())  # start collection
        col_str += "NULL,"  # end optimization
        col_str += "NULL,"  # pos label count
        col_str += "NULL"  # neg label count
        col_str += ");"
        self.c.execute(col_str)
        self.cnx.commit()

        return collection_no
    def step_lookup_counter(self,
                            twython_obj,
                            lc,
                            lremain):
        if lc == lremain:
            try:
                rate_limit_status = twython_obj.get_application_rate_limit_status(resources=['users'])
            except:
                time.sleep(1)
                rate_limit_status = twython_obj.get_application_rate_limit_status(resources=['users'])
            lookup_remaining = rate_limit_status['resources']['users']['/users/lookup']['remaining']
            if lookup_remaining == 0:
                reset_time = datetime.fromtimestamp(rate_limit_status['resources']['users']['/users/lookup']['reset'])
                wait_time = (reset_time - datetime.now()).total_seconds()
                if wait_time > 0:
                    print("Waiting for /user/lookup rate limit reset")
                    print("Reset is at " + datetime.strftime(reset_time, "%H:%M:%S"))
                    print("Sleeping for {0:d} seconds".format(int(wait_time)))
                    time.sleep(wait_time)
                    try:
                        rate_limit_status = twython_obj.get_application_rate_limit_status(resources=['users'])
                    except:
                        time.sleep(1)
                        rate_limit_status = twython_obj.get_application_rate_limit_status(resources=['users'])
                    lookup_remaining = rate_limit_status['resources']['users']['/users/lookup']['remaining']
            new_count = lremain - lookup_remaining
        else:
            new_count = lc + 1
        return new_count

    def get_expand_sample(self,
                          query_type: str,
                          sample_size: int,
                          connection_lower_bound: int=None,
                          connection_upper_bound: int=None,
                          min_tweet_rate: float=0,
                          prob_cutoff=None):
        if query_type not in ['friends', 'followers']:
            query_type = 'friends'  # Sets default, but hopefully this never matters
        query_str = "SELECT user_id,Prob FROM user WHERE "
        query_str += " protected = 0 "
        if connection_upper_bound is not None:
            query_str += " AND {0:s}_count <= {1:d} ".format(query_type, connection_upper_bound)
        if connection_lower_bound is not None:
            query_str += " AND {0:s}_count > {1:d}".format(query_type, connection_lower_bound)
        if min_tweet_rate > 0:
            query_str += " AND tweet_rate >= {0:1.4f}".format(float(min_tweet_rate))
        query_str += " AND queried_{0:s} != 'Y' ".format(query_type)
        query_str += " AND Label = 1"
        self.c.execute(query_str)
        result = self.c.fetchall()

        if (prob_cutoff is not None) and (len(result) > sample_size):
            result.sort(key=lambda z: z[1])
            if result[sample_size - 1][1] <= prob_cutoff:
                result = [i for i in result if i[1] <= prob_cutoff]
            else:
                result = [i for i in result if i[1] <= result[sample_size - 1][1]]
        result = [i[0] for i in result]
        if len(result) > sample_size:
            result_sample = sample(result, sample_size)
        else:
            result_sample = result
        return result_sample

    def execute_less_query(self,
                           user_id,
                           collection_no,
                           relationship_type,
                           twython_obj,
                           lookup_counter=900,
                           lookup_remaining=900):
        self.c.execute("INSERT INTO collection_queries VALUES ({0:d},{1:d},'{2:s}');".format(collection_no, user_id, relationship_type))
        self.c.execute("UPDATE user SET queried_{0:s}s='Y' WHERE user_id = {1:d};".format(relationship_type, user_id))

        if relationship_type == 'friend':
            non_relationship_type = 'follower'
            try:
                new_users = twython_obj.get_friends_list(user_id=user_id, count=200)
            except:
                time.sleep(1)
                try:
                    new_users = twython_obj.get_friends_list(user_id=user_id, count=200)
                except:
                    print("Cannot get {0:s}s list for user {1:d}".format(relationship_type, user_id))
                    new_users = {'users': []}
        else:
            non_relationship_type = 'friend'
            try:
                new_users = twython_obj.get_followers_list(user_id=user_id, count=200)
            except:
                time.sleep(1)
                try:
                    new_users = twython_obj.get_followers_list(user_id=user_id, count=200)
                except:
                    print("Cannot get {0:s}s list for user {1:d}".format(relationship_type, user_id))
                    new_users = {'users': []}
        rec_users = [j['id'] for j in new_users['users'] if j['{0:s}s_count'.format(non_relationship_type)] == 0]
        new_users = [j for j in new_users['users'] if j['{0:s}s_count'.format(non_relationship_type)] > 0]
        if len(rec_users) > 0:
            while len(rec_users) > 0:
                try:
                    lookup_counter = self.step_lookup_counter(twython_obj, lookup_counter, lookup_remaining)
                    new_users += twython_obj.lookup_user(user_id=rec_users[0:100])
                except:
                    time.sleep(1)
                    lookup_counter = self.step_lookup_counter(twython_obj, lookup_counter, lookup_remaining)
                    try:
                        new_users += twython_obj.lookup_user(user_id=rec_users[0:100])
                    except:
                        print("Collection problem for {0:s}s of {1:d}.".format(relationship_type, user_id))
                rec_users = rec_users[100:len(rec_users)]
        return new_users, lookup_counter

    def execute_more_query(self,
                           user_id,
                           collection_no,
                           relationship_type,
                           twython_obj,
                           lookup_counter=900,
                           lookup_remaining=900):
        self.c.execute("INSERT INTO collection_queries VALUES ({0:d},{1:d},'{2:s}');".format(collection_no, user_id, relationship_type))
        self.c.execute(
            "SELECT queried_{0:s}s,{0:s}_query_page FROM user WHERE user_id={1:d};".format(relationship_type, user_id))
        result = self.c.fetchall()
        if relationship_type == 'friend':
            non_relationship_type = 'follower'
        else:
            non_relationship_type = 'friend'
        if result[0][0] == 'P':
            if relationship_type == 'friend':
                try:
                    new_user_ids_result = twython_obj.get_friends_ids(user_id=user_id, count=5000, cursor=result[0][1])
                except:
                    time.sleep(1)
                    try:
                        new_user_ids_result = twython_obj.get_friends_ids(user_id=user_id, count=5000,
                                                                          cursor=result[0][1])
                    except:
                        print("Cannot get {0:s}s ids for user {1:d}.".format(relationship_type, user_id))
                        new_user_ids_result = {'ids': [], 'next_cursor': 0}
            else:
                try:
                    new_user_ids_result = twython_obj.get_followers_ids(user_id=user_id, count=5000,
                                                                        cursor=result[0][1])
                except:
                    time.sleep(1)
                    try:
                        new_user_ids_result = twython_obj.get_followers_ids(user_id=user_id, count=5000,
                                                                            cursor=result[0][1])
                    except:
                        print("Cannot get {0:s}s ids for user {1:d}.".format(relationship_type, user_id))
                        new_user_ids_result = {'ids': [], 'next_cursor': 0}
        else:
            if relationship_type == 'friend':
                try:
                    new_user_ids_result = twython_obj.get_friends_ids(user_id=user_id, count=5000)
                except:
                    time.sleep(1)
                    try:
                        new_user_ids_result = twython_obj.get_friends_ids(user_id=user_id, count=5000)
                    except:
                        print("Cannot get {0:s}s ids for user {1:d}.".format(relationship_type, user_id))
                        new_user_ids_result = {'ids': [], 'next_cursor': 0}
            else:
                try:
                    new_user_ids_result = twython_obj.get_followers_ids(user_id=user_id, count=5000)
                except:
                    time.sleep(1)
                    try:
                        new_user_ids_result = twython_obj.get_followers_ids(user_id=user_id, count=5000)
                    except:
                        print("Cannot get {0:s}s ids for user {1:d}.".format(relationship_type, user_id))
                        new_user_ids_result = {'ids': [], 'next_cursor': 0}
        new_user_ids = new_user_ids_result['ids']
        if new_user_ids_result['next_cursor'] == 0:
            self.c.execute("UPDATE user SET queried_{0:s}s = 'Y', {0:s}_query_page=NULL WHERE user_id = {1:d};".format(
                relationship_type, user_id))
        else:
            self.c.execute(
                "UPDATE user SET queried_{0:s}s = 'P', {0:s}_query_page = {1:d} WHERE user_id= {2:d};".format(
                    relationship_type, new_user_ids_result['next_cursor'], user_id))
        new_users = []
        while len(new_user_ids) > 0:
            try:
                lookup_counter = self.step_lookup_counter(twython_obj, lookup_counter, lookup_remaining)
                add_users = twython_obj.lookup_user(user_id=new_user_ids[0:100])
            except:
                time.sleep(1)
                lookup_counter = self.step_lookup_counter(twython_obj, lookup_counter, lookup_remaining)
                try:
                    add_users = twython_obj.lookup_user(user_id=new_user_ids[0:100])
                except:
                    print("Error Collecting {0:s} profiles for user {1:d}".format(relationship_type, user_id))
                    add_users = []
            if len(add_users) > 0 and all([j['{0:s}s_count'.format(non_relationship_type)] == 0 for j in add_users]):
                try:
                    lookup_counter = self.step_lookup_counter(twython_obj, lookup_counter, lookup_remaining)
                    add_users = twython_obj.lookup_user(user_id=new_user_ids[0:100])
                except:
                    time.sleep(1)
                    lookup_counter = self.step_lookup_counter(twython_obj, lookup_counter, lookup_remaining)
                    try:
                        add_users = twython_obj.lookup_user(user_id=new_user_ids[0:100])
                    except:
                        print("Error Collecting {0:s} profiles for user {1:d}".format(relationship_type, user_id))
            new_users += add_users
            new_user_ids = new_user_ids[100:len(new_user_ids)]
        return new_users, lookup_counter

    def expand_step(self,
                    twython_obj,
                    features_func,
                    sample_size=15,
                    prob_cutoff=0.5):

        collection_no = self.add_collection_start()
        try:
            rate_limit_status = twython_obj.get_application_rate_limit_status(resources=['users'])
        except:
            time.sleep(1)
            rate_limit_status = twython_obj.get_application_rate_limit_status(resources=['users'])
        lookup_remaining = 900
        lookup_counter = lookup_remaining - rate_limit_status['resources']['users']['/users/lookup']['remaining']
        friends_more = self.get_expand_sample('friends', sample_size, 200, None, 0)
        friends_less = self.get_expand_sample('friends', (30 - len(friends_more)), 0, 200, 0)
        followers_more = self.get_expand_sample('followers', sample_size, 200, None, 0)
        followers_less = self.get_expand_sample('followers', (30 - len(followers_more)), 0, 200, 0)
        while (len(friends_less) > 15):
            friends_more.append(friends_less.pop(0))
        while (len(followers_less) > 15):
            followers_more.append(followers_less.pop(0))
        for userid in friends_less:
            new_users, lookup_counter = self.execute_less_query(userid, collection_no, 'friend', twython_obj,
                                                                lookup_counter, lookup_remaining)
            if len(new_users) > 0:
                self.add_users(new_users, features_func, collection_no)
                self.add_links(userid, new_users, 'friend')
        for userid in friends_more:
            new_users, lookup_counter = self.execute_more_query(userid, collection_no, 'friend', twython_obj,
                                                                lookup_counter, lookup_remaining)
            while len(new_users) > 0:
                us_1000 = new_users[0:1000]
                self.add_users(us_1000, features_func, collection_no)
                self.add_links(userid, us_1000, 'friend')
                new_users = new_users[1000:len(new_users)]
        for userid in followers_less:
            new_users, lookup_counter = self.execute_less_query(userid, collection_no, 'follower', twython_obj,
                                                                lookup_counter)
            if len(new_users) > 0:
                self.add_users(new_users, features_func, collection_no)
                self.add_links(userid, new_users, 'follower')
        for userid in followers_more:
            new_users, lookup_counter = self.execute_more_query(userid, collection_no, 'follower', twython_obj,
                                                                lookup_counter)
            while len(new_users) > 0:
                us_1000 = new_users[0:1000]
                self.add_users(us_1000, features_func, collection_no)
                self.add_links(userid, us_1000, 'follower')
                new_users = new_users[1000:len(new_users)]
        return self.user_count

    def collect_geo_users(self,
                          twython_obj,
                          max_time_collecting=72 * 60 * 60,
                          min_num_geo=-1):
        if min_num_geo == -1:
            min_num_geo = self.user_count

        self.c.execute("SELECT COUNT(1) from geo_user WHERE geo_tweet=1;")
        result = self.c.fetchall()
        num_geo = result[0][0]

        self.c.execute(
            "SELECT user.user_id FROM user LEFT OUTER JOIN geo_user ON user.user_id=geo_user.user_id WHERE user.geo_enabled = 1 AND user.protected=0 AND user.tweet_rate > 0 AND geo_user.geo_tweet IS NULL;")
        result = self.c.fetchall()
        unqueried_geo_ids = [i[0] for i in result]

        num_unqueried_geo_ids = len(unqueried_geo_ids)
        shuffle(unqueried_geo_ids)
        collect_end_time = datetime.now() + timedelta(0, max_time_collecting)
        iteration_start_time = datetime.now()
        count = 0
        while (num_geo < min_num_geo) and (datetime.now() < collect_end_time) and (len(unqueried_geo_ids) > 0):
            time.sleep(max(0, 1 - (datetime.now() - iteration_start_time).total_seconds()))
            iteration_start_time = datetime.now()
            user_id = unqueried_geo_ids.pop()
            num_unqueried_geo_ids += (-1)
            num_geo += self.add_geo_user(user_id, twython_obj)
            count += 1
            if count % 500 == 0:
                print("Ongoing Geo-Collection.")
                print("Queries executed in this collection: {0:d}".format(count))
                print("Remaining unqueried geo-users: {0:d}".format(num_unqueried_geo_ids))
                print("Total Geo-tweets found so far: {0:d}".format(num_geo))
                print("Stop if {0:d} geo-tweets are found".format(min_num_geo))
                print("Stop no later than {0:s}".format(collect_end_time.strftime('%H:%M:%S, %B %d')))
                sys.stdout.flush()
        sys.stdout.flush()

    def _classify_get_user_data(self,
                                phi_func: Callable) -> dict:
        """
        Get user data for classification step.
        From SQL select, get a table with rows containing the following fields (in order):
        - 0: friend count
        - 1: follower count
        - 2: user id
        - 3 to _n_: vector of _n-3_ coefficients for phi_func (defined elsewhere)

        :param phi_func:
        :return: mapping of user_id to dict of parsed data
        """
        self.c.execute(
            "SELECT user.friends_count,user.followers_count,features.*  FROM user INNER JOIN features ON user.user_id=features.user_id WHERE user.Optim=1;"
        )
        result = self.c.fetchall()

        user_data = {
            doc[2]: {
                "user_id": doc[2],
                "friends_count": doc[0],
                "followers_count": doc[1],
                "phi": phi_func([int(j) for j in doc[3:len(doc)]]),
                "a00_contrib": 0
            } for doc in result
        }
        return user_data

    def _classify_get_link_data(self) -> list:
        """
        Get link data for classification step.

        :return: list of links, where a link has the following fields:
                 - 0: left_user_id
                 - 1: right_user_id
                 - 2: (bool) left_follows_right
                 - 3: (bool) right_follows_left
        """
        self.c.execute(
            "SELECT link.left_user_id,link.right_user_id,link.left_follows_right,link.right_follows_left FROM user AS u1 INNER JOIN link ON u1.user_id=link.left_user_id INNER JOIN user AS u2 ON link.right_user_id = u2.user_id WHERE u1.Optim=1 AND u2.Optim=1;")
        link_data = self.c.fetchall()
        return [_ClassifyLinkData(*link) for link in link_data]

    def classify_step(self,
                      phi_func: Callable,
                      psi_func: Callable,
                      prune_capacity_threshold: float=log(1.5)) -> tuple:
        """

        :param phi_func: user profile energy function
        :param psi_func: link probability energy function
        :param prune_capacity_threshold: minimum value of link energy summed
                                         over all neighbors for a user to be
                                         included in future classification
                                         steps
        :return: (number of users in set 1, number of pruned users)
        """
        user_data = self._classify_get_user_data(phi_func)
        link_data = self._classify_get_link_data()

        edgelist_data = [
            _ClassifyEdgeData(left_user_id=link.left_user_id,
                              right_user_id=link.right_user_id,
                              psi=_ClassifyPsiData(*psi_func(link.left_follows_right,
                                                             link.right_follows_left,
                                                             user_data[link.left_user_id],
                                                             user_data[link.right_user_id])))
            for link in link_data
        ]

        G = nx.Graph()
        # Populate graph with nodes:
        # - one node per user in user_data (labeled with user_id)
        # - source and sink nodes (labeled 1 and 0 respectively)
        G.add_nodes_from([userid for userid in user_data.keys()] + [_SINK, _SOURCE])

        # Add edges to graph
        # Link edges: add edges between user nodes
        for edge in edgelist_data:
            alp = edge.psi.p00 * 0.5
            user_data[edge.left_user_id]["phi"] += alp  # Added: increases likelihood of set 1
            user_data[edge.right_user_id]["phi"] += alp  # Added: increases likelihood of set 1.

            a00_c = min(edge.psi.p01, edge.psi.p10) * 0.5  # alp = a00_lambda times this number
            user_data[edge.left_user_id]['a00_contrib'] += a00_c  # This will enable us to optimize for a00_lambda
            user_data[edge.right_user_id]['a00_contrib'] += a00_c

            G.add_edge(edge.left_user_id, edge.right_user_id, capacity=edge.psi.p10 - alp)

        for userid, user in user_data.items():
            if user['phi'] > 0:
                # More likely to be in set 1: put edge with positive capacity from source (less likely to cut off from source)
                G.add_edge(_SOURCE, userid, capacity=user['phi'])
            elif user['phi'] < 0:
                # More likely to be in set 0: put edge with positive capacity to sink (less likely to cut off from sink)
                G.add_edge(userid, _SINK, capacity=-user['phi'])
            else:
                # if in the 0 probability event capacity is 0, do not put in any edges from source or to sink
                pass

        # Perform min cut; mc = node partition
        cut_value, mc = nx.minimum_cut(G, _SOURCE, _SINK)
        PL = list(mc[0])  # Assumes no input indices
        if 1 not in PL:
            PL = list([i for i in mc if 1 in i][0])

        # Update labels of users in set 1
        sendsize = 2000
        current_index = 0
        while current_index < len(PL):
            pl_string = 'UPDATE user SET Label=1 WHERE user_id IN (' + str(PL[current_index])
            for uid in PL[(current_index + 1):(current_index + sendsize)]:
                pl_string += "," + str(uid)
            pl_string += ");"
            self.c.execute(pl_string)
            self.cnx.commit()
            current_index += sendsize

        pruned_nodes = []
        for userid in user_data.keys():
            neighbors = [i for i in nx.all_neighbors(G, userid) if i not in [0, 1]]
            ein = [i for i in neighbors if i in PL]
            eout = list(set(neighbors) - set(ein))
            total_neighbor_capacity = sum([G[userid][i]['capacity'] for i in neighbors])
            if total_neighbor_capacity < prune_capacity_threshold:
                pruned_nodes.append(userid)
            psi_l = sum([G.edge[userid][i]['capacity'] for i in ein]) - sum([G.edge[userid][j]['capacity'] for j in eout])
            if (user_data[userid]['phi'] + psi_l) > 12:
                user_data[userid]['prob'] = 0
            else:
                # Probability in the target (0) class
                user_data[userid]['prob'] = 1. / (exp(user_data[userid]['phi'] + psi_l) + 1)

            # user_data[userid]['phis']=phi_l
            user_data[userid]['psi_local'] = psi_l

        # Update database records for all nodes with calculated values
        counter = 0
        n_add = 5000
        user_ids = [userid for userid in user_data.keys()]
        while counter < len(user_data):
            user_ids_add = user_ids[counter:(counter + n_add)]
            updatestr = "UPDATE user SET Prob = CASE \n"
            for j in user_ids_add:
                updatestr += "WHEN user_id = " + str(j) + " THEN " + str(user_data[j]['prob']) + " \n"
            updatestr += "END \n"
            updatestr += "WHERE user_id IN (" + str(user_ids_add[0])
            for j in user_ids_add[1:len(user_ids_add)]:
                updatestr += "," + str(j)
            updatestr += ");"
            self.c.execute(updatestr)
            self.cnx.commit()
            updatestr = "UPDATE user SET psi_local = CASE \n"
            for j in user_ids_add:
                updatestr += "WHEN user_id = " + str(j) + " THEN " + str(user_data[j]['psi_local']) + " \n"
            updatestr += "END \n"
            updatestr += "WHERE user_id IN (" + str(user_ids_add[0])
            for j in user_ids_add[1:len(user_ids_add)]:
                updatestr += "," + str(j)
            updatestr += ");"
            self.c.execute(updatestr)
            self.cnx.commit()

            updatestr = "UPDATE user SET phi_local = CASE \n"
            for j in user_ids_add:
                updatestr += "WHEN user_id = " + str(j) + " THEN " + str(user_data[j]['phi']) + " \n"
            updatestr += "END \n"
            updatestr += "WHERE user_id IN (" + str(user_ids_add[0])
            for j in user_ids_add[1:len(user_ids_add)]:
                updatestr += "," + str(j)
            updatestr += ");"
            self.c.execute(updatestr)
            self.cnx.commit()

            updatestr = "UPDATE user SET a00_contrib = CASE \n"
            for j in user_ids_add:
                updatestr += "WHEN user_id = " + str(j) + " THEN " + str(user_data[j]['a00_contrib']) + " \n"
            updatestr += "END \n"
            updatestr += "WHERE user_id IN (" + str(user_ids_add[0])
            for j in user_ids_add[1:len(user_ids_add)]:
                updatestr += "," + str(j)
            updatestr += ");"
            self.c.execute(updatestr)
            self.cnx.commit()

            counter += n_add

        # Update database records for pruned nodes
        if len(pruned_nodes) > 0:
            pruned_nodes_str = 'UPDATE user SET Optim=0 WHERE user_id IN (' + str(pruned_nodes[0])
            for i in pruned_nodes[1:len(pruned_nodes)]:
                pruned_nodes_str += "," + str(i)
            pruned_nodes_str += ");"
            self.c.execute(pruned_nodes_str)
            self.cnx.commit()

        return len(PL), len(pruned_nodes)

    def update_phi_Optim0(self,
                          phi,
                          update_length=5000):
        self.c.execute(
            "SELECT features.* FROM user INNER JOIN features ON user.user_id=features.user_id WHERE user.Optim=0;")
        x = self.c.fetchall()
        user_ids = [i[0] for i in x]

        while len(x) > 0:
            updatestr = "UPDATE user SET phi_local = CASE \n"
            for j in x[0:update_length]:
                updatestr += "WHEN user_id = " + str(j[0]) + " THEN " + str(phi(j[1:len(j)])) + " \n"
            updatestr += "END \n"
            updatestr += "WHERE user_id IN (" + str(x[0][0])
            for j in x[1:update_length]:
                updatestr += "," + str(j[0])
            updatestr += ");"
            self.c.execute(updatestr)
            self.cnx.commit()

            updatestr = "UPDATE user SET Prob = CASE \n"
            for j in x[0:update_length]:
                updatestr += "WHEN user_id = " + str(j[0]) + " THEN " + str(1 / (1 + exp(phi(j[1:len(j)])))) + " \n"
            updatestr += "END \n"
            updatestr += "WHERE user_id IN (" + str(x[0][0])
            for j in x[1:update_length]:
                updatestr += "," + str(j[0])
            updatestr += ");"
            self.c.execute(updatestr)
            self.cnx.commit()

            updatestr = "UPDATE user SET a00_contrib = 0 "
            updatestr += "WHERE user_id IN (" + str(x[0][0])
            for j in x[1:update_length]:
                updatestr += "," + str(j[0])
            updatestr += ");"
            self.c.execute(updatestr)
            self.cnx.commit()

            updatestr = "UPDATE user SET psi_local = 0 "
            updatestr += "WHERE user_id IN (" + str(x[0][0])
            for j in x[1:update_length]:
                updatestr += "," + str(j[0])
            updatestr += ");"
            self.c.execute(updatestr)
            self.cnx.commit()

            x = x[update_length:len(x)]

        self.c.execute("UPDATE user SET Label=1 WHERE Optim=0 AND Prob<0.5;")
        self.c.execute("UPDATE user SET Label=0 WHERE Optim=0 AND Prob >= 0.5;")
        self.cnx.commit()

    def make_map_ROC(self,
                     savefile_name,
                     radius):
        self.c.execute(
            "SELECT user.user_id,user.Prob,user.Res,user.Label,user.Optim,geo_user.lat,geo_user.lon,geo_user.name,geo_user.screen_name,geo_user.location FROM user INNER JOIN geo_user ON user.user_id=geo_user.user_id WHERE geo_tweet=1 AND user.ML_set='TST';")
        all_latlongs = self.c.fetchall()

        if len(all_latlongs) > 0:
            all_latlongs = {
            i[0]: {'Lat': i[5], 'Long': i[6], 'Name': i[7], 'Screen_name': i[8], 'Location': i[9], 'Prob': i[1],
                   'Res': i[2], 'Label': i[3], 'Optim': i[4]} for i in all_latlongs}
            mymap = gmplot.GoogleMapPlotter(self.loclatlong[0], self.loclatlong[1], 8)
            for i in all_latlongs:
                t = str(all_latlongs[i]['Name']) + "(@" + str(all_latlongs[i]['Screen_name']) + "): " + str(
                    all_latlongs[i]['Location'])
                if all_latlongs[i]['Label']:
                    col = '#00FA9A'
                elif all_latlongs[i]['Optim']:
                    col = '#FF0000'
                else:
                    col = '#000000'
                mymap.marker(
                    all_latlongs[i]['Lat'],
                    all_latlongs[i]['Long'],
                    col,
                    title=t
                )
            mymap.circle(
                self.loclatlong[0],
                self.loclatlong[1],
                radius=radius * 5280 * 12 * 2.54 / 100,
                color='#00FA9A'
            )
            mymap.draw('./' + savefile_name + '.html')
            # ROC
            distances = {
                i:
                    self.lldist([all_latlongs[i]['Lat'], all_latlongs[i]['Long']], self.loclatlong)
                for i in all_latlongs
            }
            #### ROC
            gt = []
            for i in distances:
                gt.append([i, 1 if distances[i] < radius else 0, all_latlongs[i]['Prob'], all_latlongs[i]['Res']])
            gt.sort(key=lambda z: z[2])
            s = list(set([i[2] for i in gt]))
            s.sort()
            ins = len([i for i in gt if i[1]])
            outs = len([i for i in gt if not i[1]])
            TP = [0]
            FA = [0]
            auc = 0
            p_cutoff_x = 0
            p_cutoff_y = 0
            p_cutoff = 0.5
            finding_p = True
            for i in s:
                y0 = TP[len(TP) - 1]
                x0 = FA[len(FA) - 1]
                TP.append(len([j for j in gt if j[2] <= i and j[1]]) * 1. / ins)
                FA.append(len([j for j in gt if j[2] <= i and not j[1]]) * 1. / outs)
                y1 = TP[len(TP) - 1]
                x1 = FA[len(FA) - 1]
                if finding_p:
                    if i >= p_cutoff:
                        p_cutoff_x = x1
                        p_cutoff_y = y1
                        finding_p = False
                auc += (x1 - x0) * (y0 + y1) * 1. / 2
            plt.plot(FA, TP)
            plt.scatter(p_cutoff_x,
                        p_cutoff_y,
                        marker='D',
                        c='red',
                        s=30)
            plt.plot([0, 1], [0, 1], '--')
            if self.location_name != "":
                plt.title("ROC Plot for " + self.location_name + " Collection")
            else:
                plt.title("ROC Plot")
            plt.text(0.75, 0.05, "AUC: {0:1.3f}".format(auc), size=12)
            plt.text(p_cutoff_x, p_cutoff_y, r"$P=${0:1.3f}".format(p_cutoff), size=12)
            plt.xlabel(r"$P_{F}$")
            plt.ylabel(r"$P_{D}$")
            plt.savefig('./ROC-' + savefile_name + '.png')
            plt.clf()
            plt.close()
