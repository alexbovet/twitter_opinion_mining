# -*- coding: utf-8 -*-
"""
Created on Fri Jun 24 16:30:00 2016

@author: Alexandre Bovet <alexandre.bovet@gmail.com>

create sqlite databases for tweets

"""
import os
import time
import sqlite3
import ujson as json
from TwSentiment import CustomTweetTokenizer
from collections import Counter
import random
import pickle
import pandas as pd
import re
#raise Exception

def createTweetSqliteDB(db_connection):
    
    table_name = 'tweet'
    
    tweet_column = 'tweet_id'
    tweet_type = 'INTEGER'
    file_column = 'filename_id'
    filename_type = 'INTEGER'
    query_column = 'query_id'
    query_type = 'INTEGER'
    time_column = 'datetime_EST'
    time_type = 'TIMESTAMP'
    user_id_column = 'user_id'
    user_id_type = 'INTEGER'
    text_column = 'text'
    text_type = 'TEXT'
    place_column = 'place'
    place_type = 'TEXT'
    source_url_col = 'source_url_id'
    source_url_type = 'INTEGER'    
    source_content_col = 'source_content_id'
    source_content_type = 'INTEGER'
    
    
    c = db_connection.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                    {c1} {t1} PRIMARY KEY, 
                    {c2} {t2}, 
                    {c3} {t3}, 
                    {c4} {t4}, 
                    {c5} {t5}, 
                    {c6} {t6}, 
                    {c7} {t7}, 
                    {c8} {t8}, 
                    {c9} {t9},
                    FOREIGN KEY({c2}) REFERENCES query(id),
                    FOREIGN KEY({c3}) REFERENCES filename(id),
                    FOREIGN KEY({c8}) REFERENCES source_url(id),
                    FOREIGN KEY({c9}) REFERENCES source_content(id))""".format(tn=table_name,
              c1=tweet_column, t1=tweet_type,
              c2=query_column, t2=query_type, 
              c3=file_column, t3=filename_type,
              c4=time_column, t4=time_type,
              c5=user_id_column, t5=user_id_type,
              c6=text_column, t6=text_type,
              c7=place_column, t7=place_type,
              c8=source_url_col, t8=source_url_type,
              c9=source_content_col, t9=source_content_type))

    

def createFilenameSqliteDB(db_connection):

    table_name = 'filename'
    
    id_col = 'id'
    id_type = 'INTEGER PRIMARY KEY'
    file_column = 'filename'
    file_type = 'TEXT UNIQUE'

    c = db_connection.cursor()
    
    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                    {c1} {t1},
                    {c2} {t2}
                    )""".format(tn=table_name,
              c1=id_col, t1=id_type,
              c2=file_column, t2=file_type))
    
def createQuerySqliteDB(db_connection):
    
    table_name = 'query'
    
    id_col = 'id'
    id_type = 'INTEGER PRIMARY KEY'
    query_column = 'query'
    query_type = 'TEXT UNIQUE'

    c = db_connection.cursor()
    
    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                    {c1} {t1},
                    {c2} {t2}
                    )""".format(tn=table_name,
              c1=id_col, t1=id_type,
              c2=query_column, t2=query_type))
    

def createSourceURLSqliteDB(db_connection):
    
    table_name = 'source_url'
    
    id_col = 'id'
    id_type = 'INTEGER PRIMARY KEY'
    source_column = 'source_url'
    source_type = 'TEXT UNIQUE'

    c = db_connection.cursor()
    
    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                    {c1} {t1},
                    {c2} {t2}
                    )""".format(tn=table_name,
              c1=id_col, t1=id_type,
              c2=source_column, t2=source_type))
    
def createSourceContentSqliteDB(db_connection):
    
    table_name = 'source_content'
    
    id_col = 'id'
    id_type = 'INTEGER PRIMARY KEY'
    source_column = 'source_content'
    source_type = 'TEXT UNIQUE'

    c = db_connection.cursor()
    
    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                    {c1} {t1},
                    {c2} {t2}
                    )""".format(tn=table_name,
              c1=id_col, t1=id_type,
              c2=source_column, t2=source_type))

def createUserTableSqliteDB(db_connection):
    """ create user table """
    
  
    table_name = 'user'
    
    user_id_col = 'user_id'
    user_id_type = 'INTEGER'
    loc_col = 'first_location'
    loc_type = 'TEXT'
    time1st_col = 'first_tweet_time_EST'
    time1st_type = 'TIMESTAMP'        
    time_col = 'latest_tweet_time_EST'
    time_type = 'TIMESTAMP'
    num_tweet_col = 'num_tweet'
    num_tweet_type = 'INTEGER'


    c = db_connection.cursor()
    
    
    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                    {c1} {t1} PRIMARY KEY, 
                    {c2} {t2},
                    {c3} {t3},
                    {c4} {t4},
                    {c5} {t5})""".format(tn=table_name,
              c1=user_id_col, t1=user_id_type,
              c2=loc_col, t2=loc_type,
              c3=time1st_col, t3=time1st_type,
              c4=time_col, t4=time_type,
              c5=num_tweet_col, t5=num_tweet_type))


def createHashtagTweetUserSqliteDB(db_connection):
    """ create table for many-to-many relations between tweet_id, hashtags and 
        user_id with foreign keys constraints
    """
    
    
    table_name = 'hashtag_tweet_user'
    
    tweet_id_col = 'tweet_id'
    tweet_id_type = 'INTEGER'
    hashtag_col = 'hashtag'
    hashtag_type = 'TEXT'
    user_id_col = 'user_id'
    user_id_type = 'INTEGER'
    
    
    c = db_connection.cursor()

    # enable foreign keys
    c.execute('PRAGMA foreign_keys=on')
    
    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                                    {c1} {t1} NOT NULL, 
                                    {c2} {t2} NOT NULL,
                                    {c3} {t3},
                                    FOREIGN KEY({c1}) REFERENCES tweet(tweet_id),
                                    FOREIGN KEY({c3}) REFERENCES user(user_id),
                                    PRIMARY KEY ({c1}, {c2}))"""\
                                    .format(tn=table_name,
              c1=tweet_id_col, t1=tweet_id_type,
              c2=hashtag_col, t2=hashtag_type,
              c3=user_id_col, t3=user_id_type))
              
    

def createTweetToKeywordSqliteDB(db_connection):
    """ create table for many-to-many relations between tweet_id and keywords 
        used to retrieve them with foreign keys constraints
    """
    
    
    table_name = 'tweet_to_keyword'
    
    tweet_id_col = 'tweet_id'
    tweet_id_type = 'INTEGER'
    keyword_col = 'keyword'
    keyword_type = 'TEXT'
    
    c = db_connection.cursor()

    # enable foreign keys
    c.execute('PRAGMA foreign_keys=on')
    
    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                                    {c1} {t1} NOT NULL, 
                                    {c2} {t2},
                                    FOREIGN KEY({c1}) REFERENCES tweet(tweet_id),
                                    PRIMARY KEY ({c1}, {c2}))"""\
                                    .format(tn=table_name,
              c1=tweet_id_col, t1=tweet_id_type,
              c2=keyword_col, t2=keyword_type))
              

def createTweetToQuerySqliteDB(db_connection):
    """ create table for many-to-many relations between tweet_id and keywords 
        used to retrieve them with foreign keys constraints
    """
    
    
    table_name = 'tweet_to_query_id'
    
    tweet_id_col = 'tweet_id'
    tweet_id_type = 'INTEGER'
    query_id_col = 'query_id'
    query_id_type = 'INTEGER'
    
    c = db_connection.cursor()

    # enable foreign keys
    c.execute('PRAGMA foreign_keys=on')
    
    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                                    {c1} {t1} NOT NULL, 
                                    {c2} {t2},
                                    FOREIGN KEY({c1}) REFERENCES tweet(tweet_id),
                                    FOREIGN KEY({c2}) REFERENCES query(id),
                                    PRIMARY KEY ({c1}, {c2}))"""\
                                    .format(tn=table_name,
              c1=tweet_id_col, t1=tweet_id_type,
              c2=query_id_col, t2=query_id_type))        

def createTweetToMentionSqliteDB(db_connection):
    """ create table for many-to-many relations between tweet_id, user mentions
        in the tweet and author of the tweet with foreign key constraints
    """
    
    
    table_name = 'tweet_to_mentioned_uid'
    
    tweet_id_col = 'tweet_id'
    tweet_id_type = 'INTEGER'
    mention_col = 'mentioned_uid'
    mention_type = 'INTEGER'
    author_col = 'author_uid'
    author_type = 'INTEGER'
    
    c = db_connection.cursor()

    # enable foreign keys
    c.execute('PRAGMA foreign_keys=on')
    
    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                                    {c1} {t1} NOT NULL, 
                                    {c2} {t2},
                                    {c3} {t3},
                                    FOREIGN KEY({c1}) REFERENCES tweet(tweet_id),
                                    PRIMARY KEY ({c1}, {c2}, {c3}))"""\
                                    .format(tn=table_name,
              c1=tweet_id_col, t1=tweet_id_type,
              c2=mention_col, t2=mention_type,
              c3=author_col, t3=author_type))
              
    
def createTweetToRetweetedUserSqliteDB(db_connection):
    """ create table for one-to-many relations between tweet_id, user retweeted
        in this tweet and author of the tweet with foreign key constraints
    """
    
    
    table_name = 'tweet_to_retweeted_uid'
    
    tweet_id_col = 'tweet_id'
    tweet_id_type = 'INTEGER'
    retweet_col = 'retweeted_uid'
    retweet_type = 'INTEGER'
    author_col = 'author_uid'
    author_type = 'INTEGER'    
    retweet_id_col = 'retweet_id'
    retweet_id_type = 'INTEGER'
    
    c = db_connection.cursor()

    # enable foreign keys
    c.execute('PRAGMA foreign_keys=on')
    
    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                                    {c1} {t1} PRIMARY KEY, 
                                    {c2} {t2},
                                    {c3} {t3},
                                    {c4} {t4},
                                    FOREIGN KEY({c1}) REFERENCES tweet(tweet_id),
                                    FOREIGN KEY({c4}) REFERENCES retweeted_status(tweet_id)
                                    )"""\
                                    .format(tn=table_name,
              c1=tweet_id_col, t1=tweet_id_type,
              c2=retweet_col, t2=retweet_type,
              c3=author_col, t3=author_type,
              c4=retweet_id_col, t4=retweet_id_type))
        

def createRetweetedStatusSqliteDB(db_connection):
    """ create table containing retweeted content
    """
    
    
    table_name = 'retweeted_status'
    
    tweet_column = 'tweet_id'
    tweet_type = 'INTEGER'
    
    file_column = 'filename_id'
    filename_type = 'INTEGER'
    query_column = 'query_id'
    query_type = 'INTEGER'
    time_column = 'datetime_EST'
    time_type = 'TIMESTAMP'
    user_id_column = 'user_id'
    user_id_type = 'INTEGER'
    text_column = 'text'
    text_type = 'TEXT'
    place_column = 'place'
    place_type = 'TEXT'
    source_url_col = 'source_url_id'
    source_url_type = 'INTEGER'    
    source_content_col = 'source_content_id'
    source_content_type = 'INTEGER'
    
    c = db_connection.cursor()

    # enable foreign keys
    c.execute('PRAGMA foreign_keys=on')
    
    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                {c1} {t1} PRIMARY KEY, 
                {c2} {t2}, 
                {c3} {t3}, 
                {c4} {t4}, 
                {c5} {t5}, 
                {c6} {t6}, 
                {c7} {t7}, 
                {c8} {t8}, 
                {c9} {t9},
                FOREIGN KEY({c2}) REFERENCES query(id),
                FOREIGN KEY({c3}) REFERENCES filename(id),
                FOREIGN KEY({c8}) REFERENCES source_url(id),
                FOREIGN KEY({c9}) REFERENCES source_content(id))""".format(tn=table_name,
          c1=tweet_column, t1=tweet_type,
          c2=query_column, t2=query_type, 
          c3=file_column, t3=filename_type,
          c4=time_column, t4=time_type,
          c5=user_id_column, t5=user_id_type,
          c6=text_column, t6=text_type,
          c7=place_column, t7=place_type,
          c8=source_url_col, t8=source_url_type,
          c9=source_content_col, t9=source_content_type))
              
    
def createTweetToRepliedUserSqliteDB(db_connection):
    """ create table for one-to-many relations between tweet_id, user replied to
        in this tweet and author of the tweet with foreign key constraints
    """
    
    
    table_name = 'tweet_to_replied_uid'
    
    tweet_id_col = 'tweet_id'
    tweet_id_type = 'INTEGER'
    reply_col = 'replied_uid'
    reply_type = 'INTEGER'
    author_col = 'author_uid'
    author_type = 'INTEGER'
    
    c = db_connection.cursor()

    # enable foreign keys
    c.execute('PRAGMA foreign_keys=on')
    
    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                                    {c1} {t1} PRIMARY KEY, 
                                    {c2} {t2},
                                    {c3} {t3},
                                    FOREIGN KEY({c1}) REFERENCES tweet(tweet_id)
                                    )"""\
                                    .format(tn=table_name,
              c1=tweet_id_col, t1=tweet_id_type,
              c2=reply_col, t2=reply_type,
              c3=author_col, t3=author_type))
              
    
def createTweetToQuotedUserSqliteDB(db_connection):
    """ create table for many-to-many relations between tweet_id, user quoted
        in this tweet and author of the tweet with foreign key constraints
    """
    
    
    table_name = 'tweet_to_quoted_uid'
    
    tweet_id_col = 'tweet_id'
    tweet_id_type = 'INTEGER'
    quote_col = 'quoted_uid'
    quote_type = 'INTEGER'
    author_col = 'author_uid'
    author_type = 'INTEGER'
    
    c = db_connection.cursor()

    # enable foreign keys
    c.execute('PRAGMA foreign_keys=on')
    
    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                                    {c1} {t1} PRIMARY KEY, 
                                    {c2} {t2},
                                    {c3} {t3},
                                    FOREIGN KEY({c1}) REFERENCES tweet(tweet_id)
                                    )"""\
                                    .format(tn=table_name,
              c1=tweet_id_col, t1=tweet_id_type,
              c2=quote_col, t2=quote_type,
              c3=author_col, t3=author_type))
              
def createHashtagSqliteDB(db_connection):
    
    table_name = 'hashtag'
    
    id_col = 'id'
    id_type = 'INTEGER PRIMARY KEY'
    hashtag_col = 'hashtag'
    hashtag_type = 'TEXT UNIQUE'
    count_col = 'count'
    count_type = 'INTEGER'
    
    
    c = db_connection.cursor()


    c.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                                  {c1} {t1},
                                  {c2} {t2},
                                  {c3} {t3})""".format(tn=table_name,
              c1=id_col, t1=id_type,
              c2=hashtag_col, t2=hashtag_type,
              c3=count_col, t3=count_type))   
    
def updateTweetTableSqlite(c, tweet_values):

    c.executemany("""INSERT OR IGNORE INTO tweet (
                                tweet_id, query_id, filename_id, datetime_EST, 
                                user_id, text, place, source_url_id, source_content_id) 
                                VALUES (?,?,?,?,?,?,?,?,?)""", tweet_values)
        
def updateReTweetedStatusTableSqlite(c, retweet_values):

    c.executemany("""INSERT OR IGNORE INTO retweeted_status (
                                tweet_id, query_id, filename_id, datetime_EST, 
                                user_id, text, place, source_url_id, source_content_id) 
                                VALUES (?,?,?,?,?,?,?,?,?)""", retweet_values)
        
        
def updateQueryTableSqlite(c, query_value):
    
    c.execute("""INSERT OR IGNORE INTO query (id, query)
                     VALUES (?,?)""", query_value)


def updateFilenameTableSqlite(c, filename_value):
    
    c.execute("""INSERT OR IGNORE INTO filename (id, filename)
                     VALUES (?,?)""", filename_value)

def updateSourceURLTableSqlite(c, source_url_values):
    
    c.executemany("""INSERT OR IGNORE INTO source_url (id, source_url)
                     VALUES (?,?)""", source_url_values)

def updateSourceContentTableSqlite(c, source_content_values):
    
    c.executemany("""INSERT OR IGNORE INTO source_content (id, source_content)
                     VALUES (?,?)""", source_content_values)
    
     
def updateUserTableSqlite(c, user_values):
    
    # insert user the first time they appear (assume that tweets are fed chronologicaly)
    c.executemany("""INSERT OR IGNORE INTO user (
                                user_id, first_location, first_tweet_time_EST,
                                latest_tweet_time_EST, num_tweet)
                                VALUES (?,?,?,NULL,0)""", user_values)
        
    # update latest tweet time and num_tweet
    user_update_val = [(timestamp, user_id) for user_id, location, timestamp in user_values]
                        
    c.executemany("""UPDATE user SET latest_tweet_time_EST = ?,
                                     num_tweet = num_tweet + 1       
                     WHERE user_id = ?""",
                  user_update_val)
    
def updateHashtagTableSqlite(c, hashtag_counter):
    
    hashtags_values = [[ht] for ht in hashtag_counter.keys()]
    hashtags_counts = [(count, ht) for ht, count in hashtag_counter.items()]
    # add new hashtags
    c.executemany("""INSERT OR IGNORE INTO hashtag (
                                id, hashtag, count) VALUES (NULL,?,0)""",
                  hashtags_values)
    
    # update hashtag count
    c.executemany("UPDATE hashtag SET count = count + ? WHERE hashtag=?",
                  hashtags_counts)
    
def updateHashtagTweetUserTableSqlite(c, hashtag_tweet_user):
        
    c.executemany("""INSERT OR IGNORE INTO hashtag_tweet_user (
                                tweet_id, hashtag, user_id) VALUES (?,?,?)""",
                  hashtag_tweet_user)
    
def updateTweetToQueryTableSqliteDB(c, tweet_query_id):

    
    table_name = 'tweet_to_query_id'
    
    tweet_id_col = 'tweet_id'
    query_id_col = 'query_id'
    
    

    c.executemany("""INSERT OR IGNORE INTO {tn} (
                                {c1}, {c2}) VALUES (?,?)"""\
                    .format(tn=table_name,
                            c1=tweet_id_col,
                            c2=query_id_col),
                            tweet_query_id)
                    
def updateTweetToMentionTableSqliteDB(c, tweet_mention_author):

    
    table_name = 'tweet_to_mentioned_uid'
    
    tweet_id_col = 'tweet_id'
    mention_col = 'mentioned_uid'
    author_col = 'author_uid'

    

    c.executemany("""INSERT OR IGNORE INTO {tn} (
                                {c1}, {c2}, {c3}) VALUES (?,?,?)"""\
                    .format(tn=table_name,
                            c1=tweet_id_col,
                            c2=mention_col,
                            c3=author_col),
                            tweet_mention_author)
    
def updateTweetToRetweetedUserTableSqliteDB(c, tweet_retweeteduid_author):

    
    table_name = 'tweet_to_retweeted_uid'
    
    tweet_id_col = 'tweet_id'
    retweet_col = 'retweeted_uid'
    author_col = 'author_uid'
    retweet_id_col = 'retweet_id'

    

    c.executemany("""INSERT OR IGNORE INTO {tn} (
                                {c1}, {c2}, {c3}, {c4}) VALUES (?,?,?,?)"""\
                    .format(tn=table_name,
                            c1=tweet_id_col,
                            c2=retweet_col,
                            c3=author_col,
                            c4=retweet_id_col),
                            tweet_retweeteduid_author)

                    
def updateTweetToRepliedUserTableSqliteDB(c, tweet_replieduid_author):

    
    table_name = 'tweet_to_replied_uid'
    
    tweet_id_col = 'tweet_id'
    reply_col = 'replied_uid'
    author_col = 'author_uid'
    

    c.executemany("""INSERT OR IGNORE INTO {tn} (
                                {c1}, {c2}, {c3}) VALUES (?,?,?)"""\
                    .format(tn=table_name,
                            c1=tweet_id_col,
                            c2=reply_col,
                            c3=author_col),
                            tweet_replieduid_author)                   
    
def updateTweetToQuotedUserTableSqliteDB(c, tweet_quoteduid_author):

    
    table_name = 'tweet_to_quoted_uid'
    
    tweet_id_col = 'tweet_id'
    quote_col = 'quoted_uid'
    author_col = 'author_uid'
    

    c.executemany("""INSERT OR IGNORE INTO {tn} (
                                {c1}, {c2}, {c3}) VALUES (?,?,?)"""\
                    .format(tn=table_name,
                            c1=tweet_id_col,
                            c2=quote_col,
                            c3=author_col),
                            tweet_quoteduid_author)      
      
def updateTweetToKeywordTableSqliteDB(c, tweet_keyword):

    
    table_name = 'tweet_to_keyword'
    
    tweet_id_col = 'tweet_id'
    keyword_col = 'keyword'
    

    c.executemany("""INSERT OR IGNORE INTO {tn} (
                                {c1}, {c2}) VALUES (?,?)"""\
                    .format(tn=table_name,
                            c1=tweet_id_col,
                            c2=keyword_col),
                            tweet_keyword)


#
# ----------  main insert function  ---------------
#

def updateSqliteTables(db_connection, tweet_file,
                       filenames_dict,
                       queries_dict,
                       sources_url_dict,
                       sources_content_dict,
                       update_tweet_table=True,
                       update_retweeted_status_table=True,
                       update_user_table=True,
                       update_hashtag_table=True,
                       update_tweet_hashtag_user_table=True,
                       update_mention_table=True,
                       update_retweet_table=True,
                       update_reply_table=True,
                       update_quote_table=True,
                       update_keyword_table=True,
                       update_tweet_to_query_table=True,
                       keyword_list=None,
                       gnip_format=False,
                       sub_sample_ratio=None,
                       rand_seed=42,
                       min_date=None,
                       max_date=None):
    
    """ reads the tweets in tweet_file and insert new values in the corresponding
        sqlite tables
        
        filenames_dict and queries_dict are dictionaries mapping names to integers
    """
    
    if gnip_format:
        Tweet = __import__('GnipTweet')
    else:
        Tweet = __import__('Tweet')

    createTweetSqliteDB(db_connection)
    createRetweetedStatusSqliteDB(db_connection)
    createUserTableSqliteDB(db_connection)
    createHashtagSqliteDB(db_connection)
    createHashtagTweetUserSqliteDB(db_connection)
    createTweetToKeywordSqliteDB(db_connection)
    createTweetToMentionSqliteDB(db_connection)
    createTweetToQuotedUserSqliteDB(db_connection)
    createTweetToRepliedUserSqliteDB(db_connection)
    createTweetToRetweetedUserSqliteDB(db_connection)
    createFilenameSqliteDB(db_connection)
    createQuerySqliteDB(db_connection)
    createSourceContentSqliteDB(db_connection)
    createSourceURLSqliteDB(db_connection)
    createTweetToQuerySqliteDB(db_connection)
    
    c = db_connection.cursor()

    t0 = time.time()

    # update query and filename dict if necessary
    query = os.path.basename(os.path.dirname(os.path.normpath(tweet_file)))
    if query not in queries_dict.keys():
        queries_dict[query] = max(queries_dict.values(), default=-1)+1

    query_id = queries_dict[query]

    taj_filename = os.path.basename(os.path.normpath(tweet_file))
    if taj_filename not in filenames_dict.keys():
        filenames_dict[taj_filename] = max(filenames_dict.values(),default=-1)+1

    filename_id = filenames_dict[taj_filename]

    # update query and filename tables
    if update_tweet_table:
        updateFilenameTableSqlite(c, (filename_id, taj_filename))
        updateQueryTableSqlite(c, (query_id, query))
        
    # prepare value lists
    tweet_values = []
    retweet_status_values = []
    hashtag_counter = Counter()
    user_values = []
    hashtag_tweet_user = []
    tweet_mention_author = []
    tweet_retweeteduid_author = []
    tweet_replieduid_author = []
    tweet_quoteduid_author = []
    tweet_keyword = []
    source_url_values = []
    source_content_values = []
    tweet_query_id_values = []

    # tweet text tokenizer for keyword matching
    tokenizer = CustomTweetTokenizer(preserve_case=False, reduce_len=False, strip_handles=False, 
                 normalize_usernames=False, normalize_urls=False, keep_allupper=False)
    
    # initialize pseudo-random number sequence
    random.seed(rand_seed)

    print('... getting data from ' + tweet_file)
    
    t = time.time()
    with open(tweet_file, 'r') as fopen:
        for line in fopen:
            if sub_sample_ratio is not None:
                # process only an uniform sub-sample of the tweets
                if random.random() > sub_sample_ratio:
                    continue

                    
            tweet = json.loads(line)
            
            if update_tweet_table or update_user_table:
                location = Tweet.getTweetPlaceFullname(tweet)
                timestamp = Tweet.getTimeStamp(tweet, timezone='EST').replace(tzinfo=None)
                
                # skip of timestamp is out of time period
                if min_date is not None and timestamp < min_date:
                    continue
                    
                if max_date is not None and timestamp >= max_date:
                    continue
                
            if update_hashtag_table or update_tweet_hashtag_user_table:
                tweet_hashtags = [ht.lower() for ht in Tweet.getHashtags(tweet)]
                                  
            tweet_id = Tweet.getTweetID(tweet)
            user_id = Tweet.getUserID(tweet)
            
            
            if update_tweet_table:
                source = Tweet.getSource(tweet)
                source_url = None
                source_content = None
                if source is not None:
                    match = re.findall(r'<a href="(.*?)".*>(.*)</a>', source)
                    if match:
                        source_url = match[0][0]
                        source_content = match[0][1]
                    
                    #update sources dicts
                    if source_url not in sources_url_dict.keys():
                        sources_url_dict[source_url] = max(sources_url_dict.values(), default=-1)+1

                        source_url_values.append((sources_url_dict[source_url], source_url))
                        
                    source_url_id = sources_url_dict[source_url]
                    
                    if source_content not in sources_content_dict.keys():
                        sources_content_dict[source_content] = max(sources_content_dict.values(), default=-1)+1
                        source_content_values.append((sources_content_dict[source_content], source_content))
                
                    source_content_id = sources_content_dict[source_content]


                tweet_values.append((tweet_id,
                                   query_id,
                                   filename_id,
                                   timestamp,
                                   user_id,
                                   Tweet.getTweetText(tweet),
                                   location,
                                   source_url_id,
                                   source_content_id))
                
            if update_tweet_to_query_table:
                tweet_query_id_values.append((tweet_id, query_id))
                
            if update_user_table:
                user_values.append((user_id, location, timestamp))
            
            if update_hashtag_table:
                hashtag_counter.update(tweet_hashtags)

            
            if update_tweet_hashtag_user_table:
                hashtag_tweet_user.extend([(tweet_id, ht, user_id) for ht in tweet_hashtags])
                
            if update_mention_table:
                author_uid, mention_uids = Tweet.getMentionInfluencers(tweet)
                if len(mention_uids) > 0:
                    assert user_id == author_uid
                    tweet_mention_author.extend([(tweet_id, mention_uid, author_uid) for mention_uid in mention_uids])

            if update_retweet_table:
                author_uid, retweet_uid = Tweet.getRetweetInfluencers(tweet)
                if len(retweet_uid) > 0:
                    assert user_id == author_uid
                    assert len(retweet_uid) == 1
                    retweet_id = Tweet.getRetweetTweetID(tweet)
                    tweet_retweeteduid_author.append((tweet_id, retweet_uid[0],
                                                      author_uid, retweet_id))
                    
            if update_retweeted_status_table:
                author_uid, retweet_uid = Tweet.getRetweetInfluencers(tweet)
                if len(retweet_uid) > 0:
                    assert user_id == author_uid
                    assert len(retweet_uid) == 1
                    assert 'retweeted_status' in tweet
                    
                    # find source of the retweeted status
                    source_rt = Tweet.getSource(tweet['retweeted_status'])
                    source_url_rt = None
                    source_content_rt = None
                    if source_rt is not None:
                        match_rt = re.findall(r'<a href="(.*?)".*>(.*)</a>', source_rt)
                        if match_rt:
                            source_url_rt = match_rt[0][0]
                            source_content_rt = match_rt[0][1]
                        
                        #update sources dicts
                        if source_url_rt not in sources_url_dict.keys():
                            sources_url_dict[source_url_rt] = max(sources_url_dict.values(), default=-1)+1
    
                            source_url_values.append((sources_url_dict[source_url_rt], source_url_rt))
                            
                        source_url_rt_id = sources_url_dict[source_url_rt]
                        
                        if source_content_rt not in sources_content_dict.keys():
                            sources_content_dict[source_content_rt] = max(sources_content_dict.values(), default=-1)+1
                            source_content_values.append((sources_content_dict[source_content_rt], source_content_rt))
                    
                        source_content_rt_id = sources_content_dict[source_content_rt]

                    retweet_status_values.append((Tweet.getTweetID(tweet['retweeted_status']),
                                   query_id,
                                   filename_id,
                                   Tweet.getTimeStamp(tweet['retweeted_status'], timezone='EST').replace(tzinfo=None),
                                   Tweet.getUserID(tweet['retweeted_status']),
                                   Tweet.getTweetText(tweet['retweeted_status']),
                                   Tweet.getTweetPlaceFullname(tweet['retweeted_status']),
                                   source_url_rt_id,
                                   source_content_rt_id))
                    
                    
            if update_reply_table:
                author_uid, reply_uid = Tweet.getReplyInfluencers(tweet)
                if len(reply_uid) > 0:
                    assert user_id == author_uid
                    assert len(reply_uid) == 1
                    tweet_replieduid_author.append((tweet_id, reply_uid[0], author_uid))

            if update_quote_table:
                author_uid, quote_uid = Tweet.getQuoteInfluencers(tweet)
                if len(quote_uid) > 0:
                    assert user_id == author_uid
                    assert len(quote_uid) == 1
                    tweet_quoteduid_author.append((tweet_id, quote_uid[0], author_uid))
                
            if update_keyword_table:
                if keyword_list is None:
                    raise ValueError('You must provide a keyword list')
                else:
                    text = Tweet.getTweetText(tweet)
                    if text is not None:
                        tokens = tokenizer.tokenize(text)
                        # add version without # and @
                        tokens.extend([tok.strip('#@') for tok in tokens])
                        tokens = set(tokens)
                        for keyword in keyword_list:
                            if any(keyword.lower() == tok for tok in tokens):
                                tweet_keyword.append((tweet_id, keyword))
                        
                
                
    print('... took ' + "{:.4}".format(time.time()-t) + 's')


    print('\n*** updating sqlite tables...')
    #
    # updating tweet table
    #
    
    # insert only new tweets to avoid duplicates
    t = time.time()
    if update_tweet_table:
        updateSourceURLTableSqlite(c, source_url_values)
        updateSourceContentTableSqlite(c, source_content_values)
        
        updateTweetTableSqlite(c, tweet_values)
        
    #
    # update tweet to query_id table
    #
    if update_tweet_to_query_table:
        updateTweetToQueryTableSqliteDB(c, tweet_query_id_values)
                
    #
    # update user table
    #
    if update_user_table:
        updateUserTableSqlite(c, user_values)
                              
    #
    # updating hashtag table
    #
    if update_hashtag_table:
        updateHashtagTableSqlite(c, hashtag_counter)
        
    #
    # updating tweet to hashtag table
    #

    if update_tweet_hashtag_user_table:
        try:
            updateHashtagTweetUserTableSqlite(c, hashtag_tweet_user)
        except:
            print(hashtag_tweet_user)
    #
    # updating tweet to mention table
    #
    if update_mention_table:
        updateTweetToMentionTableSqliteDB(c, tweet_mention_author)
        
    #
    # updating retweeted status table
    #
    if update_retweeted_status_table:
        updateSourceURLTableSqlite(c, source_url_values)
        updateSourceContentTableSqlite(c, source_content_values)
        
        updateReTweetedStatusTableSqlite(c, retweet_status_values)    
        
    #
    # updating tweet to retweet table
    #
    if update_retweet_table:
        updateTweetToRetweetedUserTableSqliteDB(c, tweet_retweeteduid_author)        

    #
    # updating tweet to reply table
    #
    if update_retweet_table:
        updateTweetToRepliedUserTableSqliteDB(c, tweet_replieduid_author)     
    
    #
    # updating tweet to quote table
    #
    if update_quote_table:
        updateTweetToQuotedUserTableSqliteDB(c, tweet_quoteduid_author)

    #
    # updating tweet to keyword table
    #
    if update_keyword_table:
        updateTweetToKeywordTableSqliteDB(c, tweet_keyword)        

        
    print('*** took ' + "{:.4}".format(time.time()-t) + 's')                
        
    
#    db_connection.commit()
    
    print('\nFinished')

    print('Total time ' + "{:.6}".format(time.time()-t0) + 's')
    
def fetchgenerator(cursor, arraysize=1000):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result
            

    
def addHTSupportGroup(db_connection, ht_group_names, ht_list_lists, 
                      create_column=True, create_index=True, column_name='ht_group'):
    
    c = db_connection.cursor()
    
    # add column to hashtag_tweet_user table
    if create_column:
        c.execute("ALTER TABLE hashtag_tweet_user ADD {cname} TEXT".format(cname=column_name))
    
    for ht_group_name, ht_list in zip(ht_group_names, ht_list_lists):
        c.execute("""UPDATE hashtag_tweet_user
                          SET {cname} = '{ht_gn}' 
                          WHERE hashtag IN ({seq})"""\
                    .format(cname=column_name,
                            ht_gn=ht_group_name, seq=','.join(['?']*len(ht_list))),
                  ht_list)
        
    # create index
    if create_index:
        c.execute('CREATE INDEX IF NOT EXISTS {cname}_supp_index ON hashtag_tweet_user ({cname})'.format(cname=column_name))

    db_connection.commit()
    


        
    
def createIndexes(conn):
    c = conn.cursor()
    
    c.execute("CREATE INDEX IF NOT EXISTS timestamp_index ON tweet (datetime_EST)")
    c.execute("CREATE INDEX IF NOT EXISTS user_index ON tweet (user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS retweet_timestamp_index ON retweeted_status (datetime_EST)")
    c.execute("CREATE INDEX IF NOT EXISTS retweet_user_index ON retweeted_status (user_id)")
    
    c.execute("CREATE INDEX IF NOT EXISTS ht_count_index ON hashtag (count)")
    c.execute("CREATE INDEX IF NOT EXISTS tweet_id_index ON hashtag_tweet_user (tweet_id)")
    c.execute("CREATE INDEX IF NOT EXISTS hashtag_index ON hashtag_tweet_user (hashtag)")
    c.execute("CREATE INDEX IF NOT EXISTS user_id_index ON hashtag_tweet_user (user_id)")

    c.execute("CREATE INDEX IF NOT EXISTS tweet_id_mention_index ON tweet_to_mentioned_uid (tweet_id)")
    c.execute("CREATE INDEX IF NOT EXISTS keyword_index ON tweet_to_keyword (keyword)")

    c.execute("CREATE INDEX IF NOT EXISTS retweet_author_index ON tweet_to_retweeted_uid (author_uid)")
    c.execute("CREATE INDEX IF NOT EXISTS mention_author_index ON tweet_to_mentioned_uid (author_uid)")
    c.execute("CREATE INDEX IF NOT EXISTS reply_author_index ON tweet_to_replied_uid (author_uid)")
    c.execute("CREATE INDEX IF NOT EXISTS quote_author_index ON tweet_to_quoted_uid (author_uid)")
    c.execute("CREATE INDEX IF NOT EXISTS tweet_id_mention_index ON tweet_to_mentioned_uid (tweet_id)")
    c.execute("CREATE INDEX IF NOT EXISTS tweet_id_retweet_index ON tweet_to_retweeted_uid (tweet_id)")
    c.execute("CREATE INDEX IF NOT EXISTS retweet_id_retweet_index ON tweet_to_retweeted_uid (retweet_id)")
    c.execute("CREATE INDEX IF NOT EXISTS tweet_id_reply_index ON tweet_to_replied_uid (tweet_id)")
    c.execute("CREATE INDEX IF NOT EXISTS tweet_id_quote_index ON tweet_to_quoted_uid (tweet_id)")
    
    c.execute('CREATE INDEX IF NOT EXISTS source_content_tweet_index ON tweet (source_content_id)')
    c.execute('CREATE INDEX IF NOT EXISTS tweet_id_source_content ON source_content (source_content)')    
    c.execute('CREATE INDEX IF NOT EXISTS tweet_id_query_id_index ON tweet_to_query_id(query_id)')

        
    conn.commit()
    


