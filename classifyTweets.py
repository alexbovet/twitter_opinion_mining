__author__ = "Alexandre Bovet"


from sklearn.externals import joblib
from TwClassifier import TweetClassifier
import pickle
import time
import numpy as np
import sqlite3
import pandas as pd
from datetime import datetime

import time
import numpy as np

from ds import DS

class classifyTweets(DS):
    """ adds a table to the sqlite database with the 
        probability of classification for each tweets        
    """
    
    def run(self):
        #==============================================================================
        # PARAMETERS
        #==============================================================================
        sqlite_file = self.job['sqlite_db_filename']
        classifier_filename = self.job['classifier_filename']

        #==============================================================================
        # OPTIONAL PARAMETERS
        #==============================================================================
        propa_col_name = self.job.get('propa_col_name','p_1')

        #load classifier
        print('loading ' + classifier_filename )
        cls = joblib.load(classifier_filename)
        
        classifier = cls['sklearn_pipeline']
        label_inv_mapper = cls['label_inv_mapper']
        
        # number of tweets to classify per batch
        select_limit = 10000
        offset_start = 0
        
        # connection timout for sqlite
        conn_timeout = 60
        
        TweetClass = TweetClassifier(classifier=classifier, label_inv_mapper=label_inv_mapper)
        
        # first classify retweets, then tweets
        for CLASS_RETWEETS in [True, False]:

            if CLASS_RETWEETS:
                table_insert = 'retweet_class_proba'
                table_select = 'retweeted_status'
            else:    
                table_insert = 'class_proba'
                table_select = 'tweet'
                
            # create table in class proba db    
            with sqlite3.connect(sqlite_file, 
                                 detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn_cp:
                c_cp = conn_cp.cursor()
                c_cp.execute("""CREATE TABLE IF NOT EXISTS {tn} (
                                                             tweet_id INTEGER PRIMARY KEY,
                                                             user_id INTEGER,
                                                             {pcol} REAL
                                                             )""".format(tn=table_insert,
                                                                         pcol=propa_col_name))
                
            sql_select = """SELECT tweet_id, user_id, text FROM {tn}
                            LIMIT ? OFFSET ?""".format(tn=table_select)
            
            
            sql_insert = """INSERT OR IGNORE INTO {tn} (
                                            tweet_id, user_id, {pcol})
                                            VALUES (?,?,?)""".format(tn=table_insert,
                                                                         pcol=propa_col_name)
            # get the number of tweets (or retweets) in the table                                            
            t0 = time.time()
            with sqlite3.connect(sqlite_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
                c = conn.cursor()
                
                c.execute("SELECT COUNT(*) FROM {tbs}".format(tbs=table_select))
                
                num_row, = c.fetchall()
            
            num_row = num_row[0]
            
            t0 = time.time()
            
            # start classifying
            for i, offset in enumerate(range(offset_start, num_row, select_limit)):
                print(str(i) + ' over ' + str((num_row - offset_start)/select_limit))
                print('** row : ' + str(offset) + ' to ' + str(offset+select_limit-1))
                print('\ntotal time : ' + str(time.time() - t0))
                print('getting tweets from {tbs}'.format(tbs=table_select))
                with sqlite3.connect(sqlite_file, timeout=conn_timeout,
                                     detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
                    c = conn.cursor()
                    c.execute('PRAGMA synchronous = NORMAL')
                    c.execute(sql_select, (select_limit, offset))
                    df = pd.DataFrame(data=c.fetchall(), columns=['tweet_id', 'user_id', 'text'])

                
                conn.close()
                
                #compute classification probability
                predict_proba = TweetClass.classify_text(df.text.tolist(), return_pred_labels=False)
                
                #prob pro 1
                probs = predict_proba[:,1].tolist()
                
                values = [(int(tid), int(uid), float(p)) for tid, uid, p in zip(df.tweet_id.tolist(), df.user_id.tolist(), probs)]
                
                print('updating {tbn}'.format(tbn=table_insert))
                with sqlite3.connect(sqlite_file,
                                     timeout=conn_timeout,
                                     detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn_cp:
                    c_cp = conn_cp.cursor()
                    # for faster inserts
                    c_cp.execute('PRAGMA synchronous = NORMAL')
                    
                    # insert values
                    c_cp.executemany(sql_insert, values)
                    
                    conn_cp.commit()
                
            
            print('finished')
            print(time.time() - t0)



        
            
            