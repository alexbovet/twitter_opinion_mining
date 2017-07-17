__author__ = "Alexandre Bovet"

import graph_tool.all as gt
import time
from HTCoocNetwork import add_p_val_to_edges

from ds import DS

class addStatSigniHT(DS):
    """ add statistical significance to hashtag coocurence network
    """    
    
    def run(self):
        
        #==============================================================================
        # PARAMETERS
        #==============================================================================
        # filename of the graph to modify
        graph_file = self.job['graph_file']

        #==============================================================================
        # OPTIONAL PARAMETERS
        #==============================================================================
        # set the number of cores
        ncpu = self.job.get('ncpu', 6)
        
        G = gt.load_graph(graph_file)
        
        t0 = time.time()
        print('computing significance of links')
        add_p_val_to_edges(G, G.gp.Ntweets, ncpu=ncpu)
        print('finished')
        print(time.time()-t0)

        # save graph file
        G.save(graph_file, fmt='graphml')
                
        
        
        
