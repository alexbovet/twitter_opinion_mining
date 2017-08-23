__author__ = "Alexandre Bovet"

import numpy as np
import pandas as pd

from baseModule import baseModule

def run_from_ipython():
    try:
        __IPYTHON__
        return True
    except NameError:
        return False

if run_from_ipython():
    from IPython.display import display    
    
class selectHashtags(baseModule):
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
        
        # set option to print dataframes without wraping rows
        pd.set_option('expand_frame_repr', False)
        
        # find how many camps there are
        camps = df_prop.label_init.unique()
        camps = camps[camps > 0]
        
        signi_cols = [col for col in df_prop.columns if col[:5] == 'signi']
        
#        # list of lists with the selected hashtags
#        htgs_lists = []
        for camp in camps:
            # find htgs where signi_sum_i > sum_(j!=i) signi_sum_j and that are new
#            mask = np.logical_and(2*df_prop['signi_sum' + str(camp)] > df_prop[signi_cols].sum(axis=1),
#                                  df_prop.label_init == -1)
            
            # find htgs where signi_sum_i > sum_(j!=i) signi_sum_j
            mask = 2*df_prop['signi_sum' + str(camp)] > df_prop[signi_cols].sum(axis=1)

                                  
            
            print('\n +++ hashtags in camp ' + str(camp))
            if run_from_ipython():
                
                display(df_prop.loc[mask].sort_values('count',
                                    ascending=False).head(num_top_htgs))
            else:
                print(df_prop.loc[mask].sort_values('count',
                                    ascending=False).head(num_top_htgs))
            
            

        

        
        
