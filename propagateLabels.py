__author__ = "Alexandre Bovet"

import graph_tool.all as gt
import numpy as np
from HTCoocNetwork import propagates_labels, find_vertices_from_hashtags

from baseModule import baseModule

class propagateLabels(baseModule):
    """ propagate the labels to neighbors
    """    
    
    def run(self):
        #==============================================================================
        # PARAMETERS
        #==============================================================================
        # filename of the graph for propagating
        graph_file = self.job['graph_file']
        
        # list of lists with hashtags belonging to each camp
        initial_htgs_lists = self.job['htgs_lists']
        
        # results filename        
        propag_results_filename = self.job['propag_results_filename']

        #==============================================================================
        # OPTIONAL PARAMETERS
        #==============================================================================
        # threshold for removing htgs with less than count_ratio*max(count)        
        count_ratio = self.job.get('count_ratio', 0.001)
        
        # significance threshold (keep only edges with p_val <= p0)
        p0 = self.job.get('p0', 1e-5)

        
        G = gt.load_graph(graph_file)
        # array with hashtags names
        ht_names = G.vp.names.get_2d_array([0])

        
        # list of max counts for each camp
        initial_max_counts = []
        for htgs_list in initial_htgs_lists:
            #find the maximum number of occurence for each camp
            initial_max_counts.append(np.max(G.vp.counts.a[np.in1d(ht_names, htgs_list)]).tolist())
                
        ################
        # filter network
        ################
        
        #remove nodes with not enough counts (occurence)
        Gfilt = gt.GraphView(G, 
                    vfilt=lambda v: G.vp.counts[v] >= count_ratio*min(initial_max_counts),
                    )
        
        
        #filter significance        
        s0 = np.log10(Gfilt.gp.p0/p0)
        #shift significance accordind to new p0 value
        Gfilt.ep.s.a  = Gfilt.ep.s.a - s0
        
        Gfilt = gt.GraphView(Gfilt, efilt=lambda e: Gfilt.ep.s[e] >= 0)

        
        G_final = gt.Graph(Gfilt, prune=True)
        
        #######################
        # propagate labels
        #######################
        
        # add initial labels tp Graph
        G_final.vp['label_init'] = G_final.new_vertex_property('int', val = -1)
        
        for label, htgs in enumerate(initial_htgs_lists):
            for v in find_vertices_from_hashtags(G_final, htgs):
                G_final.vp['label_init'][v] = label+1
        
        # propagate
        print('Propagating labels')
        df_prop = propagates_labels(G_final, init_label_vp='label_init')
        
        #save results
        print('saving results')
        df_prop.to_pickle(propag_results_filename)
        
        
        
