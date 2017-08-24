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
    r"""Vizualize the result of the label propagation through the hashtag co-occurrences network.
    
        Must be initialized with a dictionary `job` containing the key `propag_results_filename`
        
        Visualization of the results using `selectHashtags`, and updating the 
        `htgs_lists` list. This will print a list of hashtags, :math:`i`, for each camp 
        :math:`C_k` satisfying 
        
        .. math::
            \sum_{j \in C_k} s_{ij} > \sum_{j \notin C_k} s_{ij}
        
        where :math:`\{i : i \notin C_k \}` represents all the other camps than :math:`C_k`.
        
        *Optional parameters that can be added to `job`:*
        
        :num_top_htgs: number of top hashtags to be displayed in each camp.
                       (Default is 100).
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
        pd.set_option('display.max_rows', num_top_htgs)
        
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
            
            

        

        
        
