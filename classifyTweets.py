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

from baseModule import baseModule

class classifyTweets(baseModule):
    """ Classify each tweets and updates the SQLite database with the results.
    
        Must be initialized with a dictionary `job` containing keys `sqlite_db_filename`
        and  `classifier_filename`.
        
        Adds two tables `class_proba` and `retweet_class_proba` to the SQLite database
        with the result of the classification of each tweets and original retweeted status.

        *Optional parameters:*
        
        :propa_table_name_suffix: add a suffix to the created table names in order to
                                  compare different classifiers.
                                  Default is '' (empty string).
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
        # name of the column with the probability of class 1
        propa_col_name = self.job.get('propa_col_name','p_1')
        # name suffix of the table with the classification probabilities
        propa_table_name_suffix = self.job.get('propa_table_name_suffix', '')

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
                table_insert = 'retweet_class_proba' + propa_table_name_suffix
                table_select = 'retweeted_status'
            else:    
                table_insert = 'class_proba' + propa_table_name_suffix
                table_select = 'tweet'
                
            
                
            # create table in db    
            with sqlite3.connect(sqlite_file, 
                                 detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn_cp:
                c_cp = conn_cp.cursor()
                
                # check if table already exists
                c_cp.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [t for t, in c_cp.fetchall()]
                if table_insert in tables:
                    print('Table ' + table_insert + ' already exists in database.')
                    inval = input('Do you want to drop the table (irreversibly delete it) and replace it? (y/n)')
                    if inval == 'y':
                        c_cp.execute("DROP TABLE {tn}".format(tn=table_insert))
                        conn_cp.commit()
                    else:
                        raise Exception("Cannot continue. Change `propa_table_name_suffix` to create a different table.")
                
                
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
            with sqlite3.connect(sqlite_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
                c = conn.cursor()
                
                c.execute("SELECT COUNT(*) FROM {tbs}".format(tbs=table_select))
                
                num_row, = c.fetchall()
            
            num_row = num_row[0]
            
            t0 = time.time()
            
            # start classifying using bunch of `select_limit`
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
            self.print_elapsed_time(t0)



        
            
            