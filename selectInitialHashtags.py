__author__ = "Alexandre Bovet"

import graph_tool.all as gt

from baseModule import baseModule

class selectInitialHashtags(baseModule):
    """ Select the initial set of hashtags
        
    """    
    
    def run(self):
        """ displays the list of hashtags sorted by occurences"""
        #==============================================================================
        # PARAMETERS
        #==============================================================================
        # filename of the graph for propagating
        graph_file = self.job['graph_file']        
        
        #==============================================================================
        # OPTIONAL PARAMETERS
        #==============================================================================
        # number of top hashtags to display 
        num_top_htgs = self.job.get('num_top_htgs',100)
        
        ##########################
        # print hashtag list
        ##########################


        G = gt.load_graph(graph_file)
        # array with hashtags names
        ht_names = G.vp.names.get_2d_array([0])
        counts = G.vp.counts.a
        
        ht_names_counts = sorted([(str(htn), int(htc)) for htn, htc in zip(ht_names, counts)],
                                  key=lambda x:x[1], reverse=True)
        
        print(" Top " + str(num_top_htgs) + " occuring hashtags:")
        print("* rank: (name: frequency)")
        
        for i,hnc in enumerate(ht_names_counts[:num_top_htgs]):
            print(str(i) + ' :' + str(hnc))
                                   
        
        
        
