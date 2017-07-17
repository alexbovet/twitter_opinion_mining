__author__ = "Alexandre Bovet"


from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import KFold, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.externals import joblib

import time
import numpy as np
import ujson as json

from ds import DS

class crossValOptimize(DS):
    """ optimize classifier hyper parameters
        
    """
    
    def run(self):
        #==============================================================================
        # PARAMETERS
        #==============================================================================
        features_vect_file = self.job['features_vect_file']
        labels_vect_file = self.job['labels_vect_file']
        best_params_file = self.job['best_params_file']
        
        # loading the memmaped  features
        X = joblib.load(features_vect_file)
    
        y = joblib.load(labels_vect_file)
        
        #==============================================================================
        # OPTIONAL PARAMETERS
        #==============================================================================
        # parameters
        ncpu = self.job.get('ncpu', 6)
        # score to optimize
        auto_scoring = self.job.get('auto_scoring', 'f1_micro')
        
        # number of folds for the cross val
        n_splits = self.job.get('n_splits', 10)
        
        # loss function (log = logistic regression)
        loss = self.job.get('loss', 'log')
        
        # regularization (l2 = Ridge (L2 norm))
        penalty = self.job.get('penalty', 'l2')
        
        # number of iterations of the stochastic gradient descent
        # SGD should see aounrd 1e6 samples
        n_iter = self.job.get('n_iter', int(np.ceil(5e6/X.shape[0])))

        # parameters to optimize, defaults : alpha = regularization strength
        grid_search_parameters =  self.job.get('grid_search_parameters',
                                               {'classifier__alpha' : np.logspace(-1,-7, num=20)})
        
        verbose = self.job.get('CV_verbose', 1)
        
        
        
        # classifier pipeline
        pipeline_list = [('classifier', SGDClassifier( verbose=verbose, 
                                                      loss=loss,
                                                      n_iter=n_iter,
                                                      penalty=penalty))]
                  
        pipeline = Pipeline(pipeline_list)
    
        kfold = KFold(n_splits=n_splits, shuffle=True, random_state=34)
        
        #
        # Auto Grid Search
        #
        grid_search = GridSearchCV(estimator=pipeline, param_grid=grid_search_parameters, cv=kfold,
                                   scoring=auto_scoring, verbose=0 , n_jobs=ncpu)
        
        print("\nPerforming grid search...")
        print("pipeline:", [name for name, _ in pipeline.steps])
        print("parameters:")
        print(grid_search_parameters)
        t0 = time.time()
        grid_search.fit(X, y)
        
        print("done in %0.3fs" % (time.time() - t0))
    
        print("\nBest score: %0.3f" % grid_search.best_score_)
        print("Best parameters set:")
        best_parameters_np = grid_search.best_estimator_.get_params()
        
        # prepare dictionary with best parameters default values
        self.best_parameters = {'classifier__loss': loss, 'classifier__penalty': penalty,
        						'classifier__n_iter': n_iter, 'classifier__alpha': 0.01}
        
        # update and print best parameters
        for param_name in sorted(grid_search_parameters.keys()):
            print("\t%s: %r" % (param_name, best_parameters_np[param_name]))
            
            # convert numpy dtypes to python types
            if hasattr(best_parameters_np[param_name], 'item'):
            	self.best_parameters[param_name] = best_parameters_np[param_name].item()
            else:
            	self.best_parameters[param_name] = best_parameters_np[param_name]
            	
        # save best params to JSON file
        with open(best_params_file, 'w') as fopen:
        	json.dump(self.best_parameters, fopen)
        
            	
        self.repportBack(0,0,self.best_parameters,'','')

        
            
            
