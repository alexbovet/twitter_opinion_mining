__author__ = "Alexandre Bovet"

import numpy as np
import pandas as pd

from ds import DS

class selectHashtags(DS):
    """ filter the hashtags according to occurences and label propagations
        
        Loads the results of labelPropagation and returns a list of hashtags lists
        that can be fed back to labelPropagation
        
    """    
    
    def run(self):
        #==============================================================================
        # PARAMETERS
        #==============================================================================
        # filename of the graph for propagating
        propag_results_filename = self.job['propag_results_filename']
        
        df_prop = pd.read_pickle(propag_results_filename)
        
        #==============================================================================
        # OPTIONAL PARAMETERS
        #==============================================================================        
        # number of top hashtags to display 
        num_top_htgs = self.job.get('num_top_htgs', 100)
        
        ##########################
        # print hashtag list
        ##########################
        
        # find how many camps there are
        camps = df_prop.label_init.unique()
        camps = camps[camps > 0]
        
        signi_cols = [col for col in df_prop.columns if col[:5] == 'signi']
        
        # list of lists with the selected hashtags
        htgs_lists = []
        for camp in camps:
            # find htgs where signi_sum_i > sum_(j!=i) signi_sum_j and that are new
            mask = np.logical_and(2*df_prop['signi_sum' + str(camp)] > df_prop[signi_cols].sum(axis=1),
                                  df_prop.label_init == -1)
                                  
            
            print('\n +++ hashtags in camp ' + str(camp))
            print(df_prop.loc[mask].sort_values('count',
                                    ascending=False).head(num_top_htgs))
            
            #########################
            # Here is where user should select which hashtags he wants to keep
            # or remove
            #########################
            
            # no selections, just take all of them
            
            # old htgs
            ht_list = df_prop.loc[df_prop.label_init == camp]['name'].tolist()
            # + new htgs
            ht_list.extend(df_prop.loc[mask].sort_values('count',
                                      ascending=False)['name'].head(num_top_htgs).tolist())
            
            htgs_lists.append(ht_list)

        

        
        
