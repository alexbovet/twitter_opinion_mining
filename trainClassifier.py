__author__ = "Alexandre Bovet"


from sklearn.linear_model import SGDClassifier
from sklearn.pipeline import Pipeline
from sklearn.externals import joblib
from sklearn.feature_extraction import DictVectorizer
import pickle

import time
import numpy as np
import ujson as json

from baseModule import baseModule

class trainClassifier(baseModule):
    """ train classifier
        
    """
    
    def run(self):
        #==============================================================================
        # PARAMETERS
        #==============================================================================
        features_file = self.job['features_pickle_file']
        labels_file = self.job['labels_pickle_file']
        best_params_file = self.job['best_params_file']
        labels_mappers_file = self.job['labels_mappers_file']
        classifier_filename = self.job['classifier_filename']
        
        
        # load best parameters from crossval
        with open(best_params_file, 'r') as fopen:
            best_parameters = json.load(fopen)
        
        # set classifier parameters
        # loss function (log = logistic regression)
        loss=best_parameters.get('classifier__loss', 'log')
        
        # regularization (l2 = Ridge (L2 norm))
        penalty=best_parameters.get('classifier__penalty', 'l2')
        
        # regularization strength
        alpha = best_parameters.get('classifier__alpha', 0.1)
        
        # class weight (if training set is not balanced)
        class_weight = best_parameters.get('classifier__class_weight', None)
        
        
        # number of iterations of the stochastic gradient descent
        # SGD should see aounrd 1e6 samples
        n_iter = best_parameters.get('classifier__n_iter', 100)
            
        pipeline_list = [('feat_vectorizer', DictVectorizer(dtype=np.int8, sparse=True, sort=False)),
                         ('classifier', SGDClassifier(loss=loss,
                                   alpha=alpha,
                                   n_iter=n_iter,
                                   penalty=penalty,
                                   class_weight=class_weight,
                                   shuffle=True,
                                   random_state=42))]
        
        print('Training classifier with the following parameters:')
        print('    loss         = ' + str(loss))
        print('    alpha        = ' + str(alpha))
        print('    n_iter       = ' + str(n_iter))
        print('    penalty      = ' + str(penalty))
        print('    class_weight = ' + str(class_weight) +'\n')
                  
        pipeline = Pipeline(pipeline_list)

        with open(labels_mappers_file, 'rb') as fopen:
            labels_mappers = pickle.load(fopen)
            
        label_mapper = labels_mappers['label_mapper']            
        label_inv_mapper = labels_mappers['label_inv_mapper']
        
        
        with open(features_file, 'rb') as fopen:
            features = pickle.load(fopen)
        
        with open(labels_file, 'rb') as fopen:
            labels = pickle.load(fopen)
        
        def label_transformer(labels, mapper=label_mapper):
            """ maps label names to label values """
            return np.array([mapper[l] for l in labels])    
            
        y = label_transformer(labels)

        pipeline = Pipeline(pipeline_list)
        
        print('fitting classifier')
        t0 = time.time()
        
        pipeline.fit(features, y)
        
        self.print_elapsed_time(t0) 
                                                     
        to_dump = {'sklearn_pipeline' : pipeline,
                   'label_mapper' : label_mapper,
                   'label_inv_mapper' : label_inv_mapper}
        
        joblib.dump(to_dump, classifier_filename)
        

        
            
            
