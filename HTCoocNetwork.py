# -*- coding: utf-8 -*-

# Author: Alexandre Bovet <alexandre.bovet@gmail.com>
# License: BSD 3 clause

"""
functions related to thh hashtag cooccurence network

Coocurrence significance based on 
Martinez-Romo, J. et al. Phys. Rev. E - Stat. Nonlinear, Soft Matter Phys. 84, 1–8 (2011)


"""

import graph_tool.all as gt
import numpy as np
import time
from sklearn.externals.joblib import Parallel, delayed
from functools import partial
import pandas as pd



def first_seq(N,n1,j):
    return 1 - n1/(N-j)
    
def second_seq(N,n1,n2,k,j):
    return (n1-j)*(n2-j)/((N-n2+k-j)*(k-j))
    
def first_product_array(N,n1,n2,k):
    prod = np.ones(n2-k, dtype=np.float128)
    for j in range(0,n2-k-1+1):
        prod[j] = first_seq(N,n1,j)
    return prod

def second_product_array(N,n1,n2,k):
    prod = np.ones(k, dtype=np.float128)

    for j in range(0,k-1+1):
        prod[j] = second_seq(N,n1,n2,k,j)
    return prod
    
def p_val_np(N,n1,n2,r):

    assert N>= n1 and n1 >= n2 and n2 >= r
    
    _sum = 0
    for k in range(r,n2+1):
        _sum += first_product_array(N,n1,n2,k).prod()*second_product_array(N,n1,n2,k).prod()
        
    return _sum


def compute_significance(e_ij, G, p0=1e-6):
    s,t = e_ij
    e = G.edge(s,t)
    r = G.ep.weights[e]
    n2, n1 = sorted((G.vp.counts[e.source()],G.vp.counts[e.target()]))
    N = G.gp.Ntweets
    
    return np.log10(p0/p_val_np(N,n1,n2,r))

def add_p_val_to_edges(G, N, p0=1e-6, ncpu=1):
    """ add an edge vertex property 's' with the significance of the edge
        coocurence
        """
    
    G.graph_properties['p0'] = G.new_graph_property('float')
    G.graph_properties['p0'] = p0
    
    # edge significance
    sign_map = G.new_edge_property('float')

    if ncpu == 1:
        t0 = time.time()
        for i, e in enumerate(G.edges()):
            
            print(str(i) + ' over ' + str(G.num_edges()))
            print(time.time()-t0)
            # num of cooccurence
            r = G.ep.weights[e]
            # num of occurence of v1 and v2
            n2, n1 = sorted((G.vp.counts[e.source()],G.vp.counts[e.target()]))
            
            sign_map[e] = np.log10(p0/p_val_np(N,n1,n2,r))
            
    else: #multiprocessing
    
        _compute_significance = partial(compute_significance, G=G, p0=p0)
        edges_ij = [(G.vertex_index[e.source()], G.vertex_index[e.target()]) for e in G.edges()]
                 
        sign_res = Parallel(n_jobs=ncpu, verbose=1, 
                            batch_size=1)(delayed(_compute_significance)(e_ij) for e_ij in edges_ij)
        
        for sign, e_ij in zip(sign_res, edges_ij):
            s,t = e_ij
            e = G.edge(s,t)
            sign_map[e] = sign
    
    G.ep['s'] = sign_map
        

def propagates_labels(Graph, init_label_vp='label_init'):
    """ propage labels from init_label_vp to neighbours according to edge significance
        init_label_vp is a integer graph-tool vertex property, each label has
        a value > 0. Vertices without labels have a value = -1.
        
        Returns a dataframe with each vertex as row and 
        name, count, init_label_vp, vertex_id, label_sum_(label_val) and signi_sum_(label_val)
        as columns.
        
        for each label values,
        label_sum(label_val) is equal to the number of neighbor with label_val
        signi_sum(label_val) is equal to the sum of the significance of 
                the links with neighbor of label_val
    
    """
    # results dataframe
    df_propag = pd.DataFrame(columns=['name', 'count',init_label_vp,'vertex_id'])

    df_propag['name'] = Graph.vp.names.get_2d_array([0])[0]
    df_propag['count'] = Graph.vp.counts.a
    df_propag['vertex_id'] = [Graph.vertex_index[v] for v in Graph.vertices()]
    df_propag[init_label_vp] = Graph.vp[init_label_vp].a
              

    # get the different labels
    labels_values = df_propag[init_label_vp].unique()
    labels_values = labels_values[labels_values>0]
    
    #create a dictionary of vp for each labels
    final_label_vp = dict()
    for label_val in labels_values:
        final_label_vp[label_val] = {'sum_label' : Graph.new_vertex_property('float', val=0),
                                     'sum_signi' : Graph.new_vertex_property('float', val=0)}
    
    for label_val in labels_values:
        for v_i in df_propag.loc[df_propag[init_label_vp] == label_val].vertex_id:
            v = Graph.vertex(v_i)
            for e in v.out_edges():
                final_label_vp[label_val]['sum_label'][e.target()] += 1
                final_label_vp[label_val]['sum_signi'][e.target()] += Graph.ep.s[e]
                
    for label_val in labels_values:
        df_propag['label_sum' + str(label_val)] = final_label_vp[label_val]['sum_label'].a
        df_propag['signi_sum' + str(label_val)] = final_label_vp[label_val]['sum_signi'].a

    
    return df_propag
    

def find_vertices_from_hashtags(Graph, ht_list):
    """ return Graph vertices corresponding to a hashtag name
    """
    
    name_array = Graph.vp.names.get_2d_array([0])

    indices = np.where(np.in1d(name_array, ht_list))[0]
    
    vertices = [Graph.vertex(i) for i in indices]
    
    return vertices    
