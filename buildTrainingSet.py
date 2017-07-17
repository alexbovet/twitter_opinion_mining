__author__ = "Alexandre Bovet"


from TwSentiment import CustomTweetTokenizer, bag_of_words_and_bigrams
from sklearn.feature_extraction import DictVectorizer
from sklearn.externals import joblib

import pickle
import time
import numpy as np
import sqlite3
import random
import pandas as pd
from TwSentiment import official_twitter_clients


from ds import DS

class buildTrainingSet(DS):
    """ build Training Set from sqlite db using hashtag labels 
        
    """
    
    def run(self):
        
        #==============================================================================
        # PARAMETERS
        #==============================================================================
        sqlite_file = self.job['sqlite_db_filename']
        # where to save the labels and features
        features_pickle_file = self.job['features_pickle_file']
        labels_pickle_file = self.job['labels_pickle_file']
        # where to save the vectorized features and labels (for cross validation)
        features_vect_file = self.job['features_vect_file']
        labels_vect_file = self.job['labels_vect_file']
        # where to save the functions mapping labels names to labels values
        labels_mappers_file = self.job['labels_mappers_file']
       
        #==============================================================================
        # OPTIONAL PARAMETERS
        #==============================================================================        
        column_name = self.job.get('column_name_ht_group', 'ht_class')
        
        #find label names
        with sqlite3.connect(sqlite_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            c = conn.cursor()
            c.execute("SELECT DISTINCT({col_name}) FROM hashtag_tweet_user".format(col_name=column_name))
            label_names = [ln for (ln,) in c.fetchall() if ln is not None]

        if len(label_names)>2:
            raise Exception("Cannot manage more than 2 groups")
        
        
        values_0 = sorted(label_names)
        label_0 = values_0[0]
        values_1 = sorted(label_names, reverse=True)
        label_1 = values_1[0]
        
        # select tweets in one camp but not in the other camp and that
        # are not retweets and that where sent from official twitter clients
        source_contents = "','".join(official_twitter_clients)
        
        sql_query = """SELECT text FROM tweet WHERE tweet_id IN (SELECT * FROM (
                                                                              SELECT tweet_id
                                                                              FROM hashtag_tweet_user
                                                                              WHERE {cname} == ?
                                                                              )                                            
                                                               EXCEPT
                                                               SELECT * FROM (
                                                                              SELECT tweet_id
                                                                              FROM hashtag_tweet_user
                                                                              WHERE {cname} == ?                                                       
                                                                              UNION ALL
                                                                              SELECT tweet_id
                                                                              FROM tweet_to_retweeted_uid
                                                                              )
                                                               )
                                               AND source_content_id IN (
                                                                         SELECT id 
                                                                         FROM source_content
                                                                         WHERE source_content IN ('{sc_seq}')
                                                                         )
                                                               """.format(cname=column_name,
                                                                          sc_seq=source_contents)
        
        
        
        with sqlite3.connect(sqlite_file,
                             detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            c = conn.cursor()
            
            # fist camp
            c.execute(sql_query, values_0)
            
            tweet_texts_pro_1 = [t for (t,) in c.fetchall() if t[:2] != 'RT']
                                     
            #get hashtags
            c.execute("SELECT DISTINCT hashtag FROM hashtag_tweet_user WHERE {cname} = ?".format(cname=column_name),
                          [label_0])
            
            htgs_pro_1 = [ht for (ht,) in c.fetchall()]
               
            # second camp
            
            c.execute(sql_query, values_1)
            
            tweet_texts_pro_2 = [t for (t,) in c.fetchall() if t[:2] != 'RT']
            
            c.execute("SELECT DISTINCT hashtag FROM hashtag_tweet_user WHERE {cname} = ?".format(cname=column_name),
                          [label_1])
            
            htgs_pro_2 = [ht for (ht,) in c.fetchall()]
            
     
                          
        
        #%% balance set
        print('Num tweets 1: ' + str(len(tweet_texts_pro_1)))
        print('Num tweets 2: ' + str(len(tweet_texts_pro_2)))
                       
        num_tweets = min( len(tweet_texts_pro_1), len(tweet_texts_pro_2))
        
        
        # sub sample pro_trump
        random.seed(19)
        
        tweet_texts_pro_1_sample = random.sample(tweet_texts_pro_1, num_tweets)
        tweet_texts_pro_2_sample = random.sample(tweet_texts_pro_2, num_tweets)
        
        feats_dict_list = [{'label': label_0, 'text': text} for text in tweet_texts_pro_1_sample]
        feats_dict_list.extend([{'label': label_1, 'text': text} for text in tweet_texts_pro_2_sample])
        
        df = pd.DataFrame.from_dict(feats_dict_list)
        

        
        #%% features extraction
        
        all_hashtags = []
        all_hashtags.extend(htgs_pro_1)
        all_hashtags.extend(htgs_pro_2)
        
        
        def process_tweet(tweet_text, tokenizer=CustomTweetTokenizer(preserve_case=False,
                                         reduce_len=True, 
                                         strip_handles=False,
                                         normalize_usernames=False, 
                                         normalize_urls=False, 
                                         keep_allupper=False),
                            feature_extractor=bag_of_words_and_bigrams):
            
            tokens = tokenizer.tokenize(tweet_text)
            tokens_to_keep = []
            for token in tokens:
                #remove hastags that are used for classifying
                if token[0] == '#':
                    if token[1:] in all_hashtags:
                        continue
                tokens_to_keep.append(token)
            
            return bag_of_words_and_bigrams(tokens_to_keep)
        
        label_mapper = {label_0: 0, label_1: 1}
        label_inv_mapper = {0 : label_0 , 1 : label_1}
        
        def label_transformer(labels, mapper=label_mapper):
            """ maps label names to label values """
        
            return np.array([mapper[l] for l in labels])
            
        labels = df.label.tolist()
        features = [process_tweet(t) for t in df.text.tolist()]
                
        with open(features_pickle_file, 'wb') as fopen:
            pickle.dump(features, fopen)
        
        with open(labels_pickle_file, 'wb') as fopen:
            pickle.dump(labels, fopen)
        
        with open(labels_mappers_file, 'wb') as fopen:
            pickle.dump({'label_mapper': label_mapper,
                         'label_inv_mapper': label_inv_mapper}, fopen)            
            
        y = label_transformer(labels)
        
        print('\nVectorizing features')
        
        t = time.time()
        
        vect = DictVectorizer(dtype=np.int8, sparse=True, sort=False)
        X = vect.fit_transform(features)
        
        print('\nfinished in ' + "{:.4}".format(time.time()-t) + 's')
        
        print('Num samples x Num features')
        print(X.shape)
        

        # memmaping of the features
        joblib.dump(X, features_vect_file)
        joblib.dump(y, labels_vect_file)

        
     