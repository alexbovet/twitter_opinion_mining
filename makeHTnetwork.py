__author__ = "Alexandre Bovet"

import time
import graph_tool.all as gt
import sqlite3
import numpy as np
import pandas as pd
from itertools import combinations
from collections import Counter

from baseModule import baseModule

class makeHTNetwork(baseModule):
    """ build a HT coocurence network
    """    
    
    def run(self):
        #==============================================================================
        # PARAMETERS
        #==============================================================================
        # filename of the existing sqlite file
        sqlite_file = self.job['sqlite_db_filename']
        
        # filename of the graph to be saved 
        graph_file = self.job['graph_file']

        #==============================================================================
        # OPTIONAL PARAMETERS
        #==============================================================================        
        #optionally provide start and stop dates
        start_date = self.job.get('start_date', None)
        stop_date = self.job.get('stop_date', None)

        # remove edges with less than weight_threshold counts
        weight_threshold = self.job.get('weight_threshold', 3)

        
        if start_date is not None and stop_date is not None:
            # filter tweet dates
            with sqlite3.connect(sqlite_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
                
                # get 
                df = pd.read_sql_query("""SELECT hashtag, tweet_id FROM hashtag_tweet_user
                                       WHERE tweet_id IN (
                                                          SELECT tweet_id FROM tweet
                                                          WHERE datetime_EST >= ?
                                                          AND datetime_EST < ?
                                                          )""", 
                                        conn, params=(start_date, stop_date))

        else:
            with sqlite3.connect(sqlite_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
                
                df = pd.read_sql_query("""SELECT hashtag, tweet_id FROM hashtag_tweet_user""", 
                                        conn)                                  
                                   
  
        
        gp = df.groupby('tweet_id')
        
        edges = []
        
        print('creating edge list')
        t0 = time.time()
        for name, group in gp:
            if len(group) > 1:
                edges.extend(list(combinations(sorted(group.hashtag),2)))
        
        ht_pair_count = Counter(edges)
        

        edges_list_weigths = np.array([(ht1, ht2, w) for (ht1, ht2),
                                       w in ht_pair_count.items() if w >=weight_threshold])
        self.print_elapsed_time(t0)
        
        print('creating graph')
        t0 = time.time()
        G = gt.Graph(directed=False)
        
        e_weights = G.new_edge_property('int') 
        
        G.vp['names'] = G.add_edge_list(edges_list_weigths, hashed=True, 
                                            string_vals=True, eprops=e_weights)
        
        e_weights.a = edges_list_weigths[:,2]
        
        G.ep['weights'] = e_weights
        
        ht_names = G.vp.names.get_2d_array([0])
        
        G.graph_properties['Ntweets'] = G.new_graph_property('int')
        G.graph_properties['Ntweets'] = df.tweet_id.unique().size
        G.graph_properties['start_date'] = G.new_graph_property('object')
        G.graph_properties['start_date'] = start_date
        G.graph_properties['stop_date'] = G.new_graph_property('object')
        G.graph_properties['stop_date'] = stop_date
        G.graph_properties['weight_threshold'] = G.new_graph_property('int')
        G.graph_properties['weight_threshold'] = weight_threshold
        
        self.print_elapsed_time(t0)
        
        
        #% ht counts
        count_group = df.groupby('hashtag')
        df_ht_counts = count_group.aggregate('count')
        df_ht_counts.sort_values('tweet_id', ascending=False, inplace=True)
        df_ht_counts.rename_axis({'tweet_id': 'count'},axis=1, inplace=True)
        
        df_ht_counts['id'] = np.arange(0, df_ht_counts.index.size)
        df_ht_counts['hashtag'] = df_ht_counts.index
        df_ht_counts = df_ht_counts[['id','hashtag','count']]
        
        
        #add counts to Graph vertex
        v_counts = G.new_vertex_property('int', val=0)
        ht_counts_names = np.array(df_ht_counts.hashtag.tolist())
        sorter = np.argsort(ht_counts_names)
        v_counts.a = df_ht_counts.iloc[sorter[np.searchsorted(ht_counts_names, 
                                             ht_names, sorter=sorter)]]['count']
        G.vp['counts'] = v_counts
        
        # save graph file
        G.save(graph_file, fmt='graphml')
        
        
        
        
