# Author: Alexandre Bovet <alexandre.bovet@gmail.com>
# License: BSD 3 clause


import numpy as np
import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial
import time

def applyParallel(dfGrouped, func, ncpu):
    with Pool(ncpu) as p:
        ret_list = p.map(func, [group for name, group in dfGrouped])
    return pd.concat(ret_list)


def run_from_ipython():
    try:
        __IPYTHON__
        return True
    except NameError:
        return False

if run_from_ipython():
    from IPython.display import display    

# aggregating functions: used on tweets grouped by day
def get_num_tweets(group, parallel=True):
    """ returns the number of tweets in each camp in group """
    
    # if there is no tweets for this day
    if group.index.size == 0:
        if parallel:
            return pd.DataFrame()
        else:
            return pd.Series()
            
    else:
        data = {'n_pro_1': group.n_pro_1.sum(),
                'n_pro_0': group.n_pro_0.sum()}
        
        if parallel:
            #must return a datafram when parallel
            return pd.DataFrame(data=data, index=[group.datetime_EST.iloc[0].date()])
            
        else:
            return pd.Series(data=data)
            

def get_pro_h_ratio(ggroup):
    """ returns the ratio of tweets pro 1 in ggroup"""
    return ggroup.n_pro_1.sum()/ggroup.n_pro_1.size
    

def get_num_users(group, r_threshold=0.5, parallel=True):
    """ returns the number of users pro 1 in group with a ratio of
        tweets pro of at least r_threshold
    """
    
    if group.index.size == 0:
        if parallel:
            return pd.DataFrame()
        else:
            return pd.Series()
                
    else:
        # group tweets per users
        g_users = group.groupby('user_id')
        # ratio of pro hillary tweets for each user
        pro_h_ratio = g_users.apply(get_pro_h_ratio)
       
        n_pro_1 = (pro_h_ratio > r_threshold).sum()
        n_pro_0 = (pro_h_ratio < 1-r_threshold).sum()
        n_null = pro_h_ratio.size - n_pro_1 - n_pro_0

        data = {'n_pro_1': n_pro_1, 
                'n_pro_0': n_pro_0,
                'null': n_null}
                    
        if parallel:
            return pd.DataFrame(data=data, index=[group.datetime_EST.iloc[0].date()])
        else:
            return pd.Series(data=data)
            
from baseModule import baseModule

class analyzeProbaDF(baseModule):
    """ Computes the number of tweets and the number of users in each camp per day.
    
        Must be initialized with a dictionary `job` containing keys `df_proba_filename`,
        `df_num_tweets_filename` and `df_num_users_filename`.
        
        `analyzeProbaDF` reads `df_proba_filename` and returns the number of tweets 
        and the number of users in each camp per day. The results are displayed and 
        saved as pandas dataframes to `df_num_tweets_filename` and `df_num_users_filename`.

        *Optional parameters:*

        :ncpu: number of cores to use. Default is number of cores of the machine 
               minus one.
        :resampling_frequency: frequency at which tweets are grouped. 
                               Default is `'D'`, i.e. daily. (see [1] for
                               different possibilities.)
        :threshold: threshold for the classifier probability (threshold >= 0.5).
                    Tweets with p > threshold are classified in camp2 and tweets with 
                    p < 1-threshold are classified in camp1. Default is 0.5.
        :r_threshold: threshold for the ratio of classified tweets needed to 
                      classify a user. Default is 0.5.
    
    
        [1] http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
    
    """
    def run(self):
        #==============================================================================
        # PARAMETERS
        #==============================================================================
        df_proba_filename = self.job['df_proba_filename']
        df_num_tweets_filename = self.job['df_num_tweets_filename']
        df_num_users_filename = self.job['df_num_users_filename']
        
        #==============================================================================
        # OPTIONAL PARAMETERS
        #==============================================================================
        propa_col_name = self.job.get('propa_col_name', 'p_1')
        ncpu = self.job.get('ncpu', cpu_count()-1)
        resampling_frequency = self.job.get('resampling_frequency', 'D') # day
        # threshold for the classifier probability
        threshold = self.job.get('threshold',0.5)
        # threshold for the ratio of classified tweets needed to classify a user
        r_threshold = self.job.get('r_threshold',0.5)
        
        if ncpu == 1:
            parallel=False
        else:
            parallel=True

        print('loading ' + df_proba_filename)
        df = pd.read_pickle(df_proba_filename)
        
        # display settings for pandas
        pd.set_option('expand_frame_repr', False)
        pd.set_option('display.max_rows', None)
        
        #% filter dataframe according to threshold
        df_filt = df.drop(df.loc[np.all([df[propa_col_name] <= threshold, df[propa_col_name] >= 1-threshold], axis=0)].index)
        df_filt['n_pro_1'] = df_filt[propa_col_name] > threshold
        df_filt['n_pro_0'] = df_filt[propa_col_name] < 1 - threshold
        
        # resample tweets per day
        resample = df_filt.groupby(pd.Grouper(key='datetime_EST',freq=resampling_frequency))
        
        print('threshold: ' + str(threshold))
        print('r_threshold: ' + str(r_threshold))
        
        # prepare funtions for parallel apply
        get_num_tweets_u = partial(get_num_tweets,
                                   parallel=parallel)
        
        get_num_users_u = partial(get_num_users, r_threshold=r_threshold,
                                  parallel=parallel)
        
        print('computing stats')
        t0 = time.time()
        
        if parallel:
            self.df_num_tweets = applyParallel(resample, get_num_tweets_u, ncpu)            
            self.df_num_users = applyParallel(resample, get_num_users_u, ncpu)
            
        else:
            self.df_num_tweets = resample.apply(get_num_tweets_u)
            self.df_num_users = resample.apply(get_num_users_u)

        #%% save dataframes            
        self.df_num_tweets.to_pickle(df_num_tweets_filename)    
        self.df_num_users.to_pickle(df_num_users_filename)
        
        print('finished')
        print(time.time() - t0)
        
        self.string_results = "\nNumber of tweets per day in each camp:\n"+\
                                self.df_num_tweets.to_string() + \
                                "\nNumber of users per day in each camp:\n"+\
                                self.df_num_users.to_string()
        
        if run_from_ipython():
            print('\nNumber of tweets per day in each camp:')
            display(self.df_num_tweets)
        
            print('\nNumber of users per day in each camp:')
            display(self.df_num_users)
            
        else:
            print('\nNumber of tweets per day in each camp:')
            print(self.df_num_tweets.to_string())
        
            print('\nNumber of users per day in each camp:')
            display(self.df_num_users.to_string())
            
        
        
        
            
            
