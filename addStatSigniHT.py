__author__ = "Alexandre Bovet"

import graph_tool.all as gt
import time
from HTCoocNetwork import add_p_val_to_edges
from multiprocessing import cpu_count

from baseModule import baseModule

class addStatSigniHT(baseModule):
    """ Compute the statistical significance of hashtag co-occurrences.
    
        Must be initialized with a dictionary `job` containing the key `graph_file`.
        
        Adds a property `s` to edges of the graph corresponding to the statistical 
        significance (`s = log10(p_0/p)`) of the co-occurence computed from a null
        model [1].
        The computation is done using `p0=1e-6` and `p0` is saved as a graph property.
        Different values of `p0` can be tested latter.
        The resulting graph is saved to `graph_file`.
        
        *Optional parameters that can be added to `job`:*
            
        :ncpu: number of processors to be used. (Default is the number of cores 
               on your machine minus 1).
        
        [1] Martinez-Romo, J. et al. Disentangling categorical relationships through 
        a graph of co-occurrences. Phys. Rev. E 84, 1–8 (2011).
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
        ncpu = self.job.get('ncpu', cpu_count()-1)
        
        self.G = gt.load_graph(graph_file)
        
        t0 = time.time()
        print('computing significance of links')
        add_p_val_to_edges(self.G, self.G.gp.Ntweets, ncpu=ncpu)
        print('finished')
        self.print_elapsed_time(t0)
        
        # save graph file
        self.G.save(graph_file, fmt='graphml')
                
        
        
        
