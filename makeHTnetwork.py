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
    """ Builds the network of hashtag co-occurrences.
    
        Must be initialized with a dictionary `job` containing keys `sqlite_db_filename`
        and `graph_file`.
    
        Reads all the co-occurences from the SQLite database and builds the network 
        of where nodes are hashtags and edges are co-occurrences. 
        The graph is a graph-tool object and is saved in graphml format to graph_file.
        Nodes of the graph have two properties: `counts` is the number of single 
        occurrences of the hashtag and `name` is the name of the hashtag.
        Edges have a property `weights` equal to the number of co-occurrences they represent.
        
        The graph has the following properties saved with it:
         - `Ntweets`: number of tweets with at least one hashtag used to build the graph.
         - `start_date` : date of the first tweet.
         - `stop_date` : date of the last tweet.
         - `weight_threshold` : co-occurrence threshold. Edges with less than `weight_threshold` co-occurrences are discarded.
        
         *Optional parameters that can be added to `job`:*
         
         :start_date: and
         :stop_date: to specify a time range for the tweets. (Default is `None`,
                     i.e. select all the tweets in the database).
         :weight_threshold: is the minimum number of co-occurences between to 
                            hashtag to be included in the graph. (Default is 3).
        

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
        self.G = gt.Graph(directed=False)
        
        e_weights = self.G.new_edge_property('int') 
        
        self.G.vp['names'] = self.G.add_edge_list(edges_list_weigths, hashed=True, 
                                            string_vals=True, eprops=e_weights)
        
        e_weights.a = edges_list_weigths[:,2]
        
        self.G.ep['weights'] = e_weights
        
        ht_names = self.G.vp.names.get_2d_array([0])
        
        self.G.graph_properties['Ntweets'] = self.G.new_graph_property('int')
        self.G.graph_properties['Ntweets'] = df.tweet_id.unique().size
        self.G.graph_properties['start_date'] = self.G.new_graph_property('object')
        self.G.graph_properties['start_date'] = start_date
        self.G.graph_properties['stop_date'] = self.G.new_graph_property('object')
        self.G.graph_properties['stop_date'] = stop_date
        self.G.graph_properties['weight_threshold'] = self.G.new_graph_property('int')
        self.G.graph_properties['weight_threshold'] = weight_threshold
        
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
        v_counts = self.G.new_vertex_property('int', val=0)
        ht_counts_names = np.array(df_ht_counts.hashtag.tolist())
        sorter = np.argsort(ht_counts_names)
        v_counts.a = df_ht_counts.iloc[sorter[np.searchsorted(ht_counts_names, 
                                             ht_names, sorter=sorter)]]['count']
        self.G.vp['counts'] = v_counts
        
        # save graph file
        self.G.save(graph_file, fmt='graphml')
        
        print('\nNumber of nodes: ' + str(self.G.num_vertices()))
        print('Number of edges: ' + str(self.G.num_edges()))
        
        
        
