__author__ = "Alexandre Bovet"

import sqlite3
import pandas as pd
import time
from TwSentiment import official_twitter_clients

from baseModule import baseModule

class makeProbaDF(baseModule):
    """ Processes the classification results and writes them to a dataframe.
        
        Must be initialized with a dictionary `job` containing keys `sqlite_db_filename`
        and `df_proba_filename.
        
        `makeProbaDF` reads the classification results from the database and processes 
        them to:
            
        - Replace the classification probability of retweets with the classification
          results of the original tweets.
        - Replace the classification probability of tweets having a hashtag of one of
          the two camps (and not of the other camp) with 0 (for camp1) or 1 (for camp2).
        - Discard tweets emanating from unoffical Twitter clients.
         
        The results are saved as a pandas dataframe in `df_proba_filename`.
         
        *Optional parameters:*
         
        :use_official_clients: whether you want to keep only tweets from official 
                                clients (`True`) or all tweets (`False`).
                                Default is `True`.
        :propa_table_name_suffix: can be changed to use the classification of 
                                   different classifiers if it was used with 
                                   `classifyTweets`.
        :column_name_ht_group: is also used if it was changed to create a different 
                                training set.
    """
    
    def run(self):
        #==============================================================================
        # PARAMETERS
        #==============================================================================
        sqlite_file = self.job['sqlite_db_filename']
        df_proba_filename = self.job['df_proba_filename']

        
        #==============================================================================
        # OPTIONAL PARAMETERS
        #==============================================================================
        # whether to use only tweets from official clients
        USE_OFFICIAL_CLIENTS = self.job.get('use_official_clients',True)
        
        propa_col_name = self.job.get('propa_col_name','p_1')
        ht_group_col_name = self.job.get('column_name_ht_group', 'ht_class')
        # name suffix of the table with the classification probabilities
        propa_table_name_suffix = self.job.get('propa_table_name_suffix', '')
        
        ######
        # define sql queries
        #####

        class_proba_db_alias = 'main'
        class_proba_table_name = 'class_proba' + propa_table_name_suffix
        retweet_class_proba_table_name = 'retweet_class_proba' + propa_table_name_suffix
            
        sql_query = """SELECT tweet.datetime_EST, tweet.tweet_id, tweet.user_id, {cpdb}.{cptb}.{pcolname} 
                       FROM tweet INNER JOIN {cpdb}.{cptb} USING (tweet_id)""".format(cpdb=class_proba_db_alias,
                                                                                      cptb=class_proba_table_name,
                                                                                      pcolname=propa_col_name)
                       
        params=None
        
        if USE_OFFICIAL_CLIENTS:
            
            sql_query += """
                           WHERE tweet.source_content_id IN (
                                                             SELECT id 
                                                             FROM source_content
                                                             WHERE source_content IN ({seq})
                                                             )
                                            """.format(seq=','.join(['?']*len(official_twitter_clients)))
            params=official_twitter_clients
            
        
        
        # for retweets
        sql_query_original_retweets = """SELECT retweeted_status.datetime_EST, retweeted_status.tweet_id,
                              retweeted_status.user_id, {cpdb}.{rcptb}.{pcolname}
                       FROM retweeted_status INNER JOIN {cpdb}.{rcptb} USING (tweet_id)""".format(cpdb=class_proba_db_alias,
                                                                                            rcptb=retweet_class_proba_table_name,
                                                                                            pcolname=propa_col_name)
        
        sql_query_retweets = """SELECT tweet.datetime_EST, tweet.tweet_id,
                              tweet.user_id, {cpdb}.{cptb}.{pcolname},
                              tweet_to_retweeted_uid.retweet_id
                       FROM tweet_to_retweeted_uid
                           INNER JOIN tweet USING (tweet_id)
                           INNER JOIN {cpdb}.{cptb} USING (tweet_id)""".format(cpdb=class_proba_db_alias,
                                                                               cptb=class_proba_table_name,
                                                                               pcolname=propa_col_name)
        
        
        # select tweets with labeled hashtags
        sql_query_hashtag = """SELECT * FROM (
                                      SELECT tweet_id
                                      FROM hashtag_tweet_user
                                      WHERE {cname} == ?
                                      )                                            
                               EXCEPT
                               SELECT * FROM (
                                      SELECT tweet_id
                                      FROM hashtag_tweet_user
                                      WHERE {cname} == ?
                                      )
                                      """.format(cname=ht_group_col_name)
                       
        #################
        # querying sqlite
        #################
        
        t0 = time.time()
        print('querying sql')
        with sqlite3.connect(sqlite_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            
            # find labels name
            c = conn.cursor()
            c.execute("SELECT DISTINCT({col_name}) FROM hashtag_tweet_user".format(col_name=ht_group_col_name))
            label_names = [ln for (ln,) in c.fetchall() if ln is not None]
            if len(label_names)>2:
                raise Exception("Cannot manage more than 2 groups")                           
                    
            print('creating df proba')
            df_proba = pd.read_sql(sql_query, conn, params=params)    
                
            print('creating df_proba_original_rt')
            df_proba_original_rt = pd.read_sql(sql_query_original_retweets, conn)
            self.print_elapsed_time(t0)
            
            print('creating df_proba_rt')
            df_proba_rt = pd.read_sql(sql_query_retweets, conn)
            self.print_elapsed_time(t0)
            
            print('creating df_proba_ht_pro_0')
            df_proba_ht_pro_0 = pd.read_sql(sql_query_hashtag, conn, params=sorted(label_names))
            self.print_elapsed_time(t0)
            
            print('creating df_proba_ht_pro_1')
            df_proba_ht_pro_1 = pd.read_sql(sql_query_hashtag, conn, params=sorted(label_names, reverse=True))
            self.print_elapsed_time(t0)
        
                                                      

        #%% correct probabilites for retweets  
        # disable warning 
        pd.options.mode.chained_assignment = None
        
        df_proba_rt.rename(columns={propa_col_name: 'proba_retweet'}, inplace=True)
        
        df_proba_original_rt.rename(columns={propa_col_name: 'proba_original'}, inplace=True)

        df_tid_rtid = df_proba_rt[['tweet_id','retweet_id']]
        # avoid converting int64 to float64
        df_tid_rtid.retweet_id = df_tid_rtid.retweet_id.astype(object)
        
        df_proba_with_retweets = pd.merge(df_proba, df_tid_rtid, how='left', on='tweet_id')
        df_proba_with_retweets.fillna(-1, inplace=True)
        
        self.df_proba_all = pd.merge(df_proba_with_retweets, df_proba_original_rt[['tweet_id','proba_original']], 
                                how='left', left_on='retweet_id', right_on='tweet_id')
        
        # copy original tweet prob to retweet prob
        self.df_proba_all.loc[self.df_proba_all.proba_original.notnull(), propa_col_name]\
                         = self.df_proba_all.loc[self.df_proba_all.proba_original.notnull(), 'proba_original']
        self.df_proba_all = self.df_proba_all[['datetime_EST', 'tweet_id_x', 'user_id', propa_col_name]]
        self.df_proba_all.rename(columns={'tweet_id_x': 'tweet_id'}, inplace=True)
                         
        #%% correct probabilites for hashtags
        
        self.df_proba_all.loc[self.df_proba_all.tweet_id.isin(df_proba_ht_pro_1.tweet_id), propa_col_name] = 1.0
        self.df_proba_all.loc[self.df_proba_all.tweet_id.isin(df_proba_ht_pro_0.tweet_id), propa_col_name] = 0.0

        #save                             
        print('saving corrected dataframe')
        self.df_proba_all.to_pickle(df_proba_filename)
        print('done')
        self.print_elapsed_time(t0)
                         

        
            
            