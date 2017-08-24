from buildDatabase import buildDatabse
from makeHTnetwork import makeHTNetwork
from selectInitialHashtags import selectInitialHashtags
from propagateLabels import propagateLabels
from addStatSigniHT import addStatSigniHT
from selectHashtags import selectHashtags
from updateHTGroups import updateHTGroups
from buildTrainingSet import buildTrainingSet
from crossValOptimize import crossValOptimize
from trainClassifier import trainClassifier
from classifyTweets import classifyTweets
from makeProbaDF import makeProbaDF
from analyzeProbaDF import analyzeProbaDF
import os

# =============================================================================
# Define filenames and directories for current job
# =============================================================================
# All the parameters are saved in a dictionary named job that will be passed to
# the different modules and that can be saved to reproduce the results. 
# Optional parameters can be added to fine tune the process. Each optional 
# parameter is explained at the corresponding step below.

# directory where project files will be saved
project_dir = 'test_proj'
os.makedirs(project_dir, exist_ok=True)

# list of directories containing the tweet archive files (TAJ)
tweet_archive_dirs = ['etrade']

# SQLite database that will be created
sqlite_file = os.path.join(project_dir, 'test.sqlite')

# hashtag co-occurrence graph that will be created
graph_file = os.path.join(project_dir, 'graph_file.graphml')

# pickle files where the training set features will be saved
features_pickle_file = os.path.join(project_dir, 'features.pickle')

# pickle file where the training set labels will be saved
labels_pickle_file = os.path.join(project_dir, 'labels.pickle')

# vectorized features file
features_vect_file = os.path.join(project_dir, 'features.mmap')

# vectorized labels file
labels_vect_file = os.path.join(project_dir, 'labels.mmap')

# mapping between labels names and numbers
labels_mappers_file = os.path.join(project_dir, 'labels_mappers.pickle')

# JSON file with the classifier best parameters obtained from cross-validation
best_params_file = os.path.join(project_dir, 'best_params.json')

# where the trained calssifier will be saved
classifier_filename = os.path.join(project_dir, 'classifier.pickle')

# DataFrame with the results of the label propagation
# on the hashtag network
propag_results_filename = os.path.join(project_dir, 'propag_results.pickle')

# DataFrame with the classification probability of
# every tweets in the database
df_proba_filename = os.path.join(project_dir, 'df_proba.pickle')

# DataFrame with the number of tweets in each camp per day
df_num_tweets_filename = os.path.join(project_dir, 'df_num_tweets.pickle')

# DataFrame with the number of users in each camp per day
df_num_users_filename = os.path.join(project_dir, 'df_num_users.pickle')


job = {'tweet_archive_dirs': tweet_archive_dirs,
       'sqlite_db_filename' : sqlite_file,
       'graph_file' : graph_file,
       'propag_results_filename' : propag_results_filename,
      'features_pickle_file': features_pickle_file,
      'labels_pickle_file': labels_pickle_file,
      'features_vect_file': features_vect_file,
      'labels_vect_file': labels_vect_file,
      'labels_mappers_file' : labels_mappers_file,
       'classifier_filename':classifier_filename,
       'df_proba_filename':df_proba_filename,
       'df_num_tweets_filename': df_num_tweets_filename,
       'df_num_users_filename': df_num_users_filename,
       'best_params_file' : best_params_file
       }

raise Exception

#%% build database
# Read the tweets from all the .taj files in the directories `tweet_archive_dirs`
# and add them to the database `sqlite_db_filename`.

buildDatabse(job).run()

#%% make HT network
# Reads all the co-occurences from the SQLite database and builds the network 
# of where nodes are hashtags and edges are co-occurrences. 
# The graph is a graph-tool object and is saved in graphml format to graph_file.
# Nodes of the graph have two properties: `counts` is the number of single 
# occurrences of the hashtag and `name` is the name of the hashtag.
# Edges have a property `weights` equal to the number of co-occurrences they represent.

# The graph has the following properties saved with it:
# - `Ntweets`: number of tweets with at least one hashtag used to build the graph.
# - `start_date` : date of the first tweet.
# - `stop_date` : date of the last tweet.
# - `weight_threshold` : co-occurrence threshold. Edges with less than `weight_threshold` co-occurrences are discarded.
#
# *Optional parameters that can be added to `job`:*
# - `start_date` and `stop_date` to specify a time range for the tweets. (Default is `None`, i.e. select all the tweets in the database).
# - `weight_threshold` is the minimum number of co-occurences between to hashtag to be included in the graph. (Default is 3).
#
# To add a parameter to job, simply execute `job["parameter name"] = parameter value`.

makeHTNetwork(job).run()


#%% add significance value to edges
# Adds a property `s` to edges of the graph corresponding to the statistical 
# significance (`s = log10(p_0/p)`) of the co-occurence computed from a null
# model [1].
# The computation is done using `p0=1e-6` and `p0` is saved as a graph property.
# Different values of `p0` can be tested latter.
# The resulting graph is saved to `graph_file`.
#
# *Optional parameters that can be added to `job`:*
# - `ncpu` : number of processors to be used. (Default is the number of cores 
# on your machine minus 1).
#
# [1] Martinez-Romo, J. et al. Disentangling categorical relationships through 
# a graph of co-occurrences. Phys. Rev. E 84, 1–8 (2011).

addStatSigniHT(job).run()


#%% select initial hashtags
# This will display to top occurring hashtags.
# 
# Optional parameters that can be added to job:
# 
#    `num_top_htgs` : (Default is top 100).

selectInitialHashtags(job).run()

#%%
# select the seeds you want to use for the label propagation
# user input
job['initial_htgs_lists'] = [['mortgage'],
                             ['etrade']]
                      

#%% propagate labels
# This part can be looped by updating the htgs_lists in job with the result of 
# the label propagation to reach a larger number of hashtags.

# start with the hashtag seeds selected above.
job['htgs_lists'] = job['initial_htgs_lists']


# The loop has two steps:
# 1. `propagateLabels` uses the graph from `graph_file` and the initial hashtags 
#    from `htgs_lists` to propagate their labels to their neighbors taking into 
#    account the statistical significance of edges. The results are saved in a 
#    pandas DataFrame in `propag_results_filename`.
#    - *Optional parameters that can be added to `job`:*
#        - `count_ratio` : threshold, $r$, for removing hashtags with a number 
#           of single occurrences smaller than $r \max\limits_{v_j\in C_k} c_j$
#           where $c_i$ is the number of occurrences of the hashtag associated
#           with vertex $v_i$, $C_k$ is the class to which $v_i$ belong. 
#           (Default = 0.001).
#        - `p0` : significance threshold. to keep only edges with p_val <= p0. 
#           (Default = 1e-5).
#
# 2. Visualisation of the results using `selectHashtags`, and updating the 
#    `htgs_lists` list. This will print a list of hashtags, $i$, for each camp 
#    $C_k$ satisfying: $\sum_{j \in C_k} s_{ij} > \sum_{j \in C_l} s_{ij}$, 
#    where $C_l$ represents all the other camps than $C_k$.
#    - *Optional parameters that can be added to `job`:*
#        - `num_top_htgs` : number of top hashtags to be displayed in each camp.
#          (Default is 100).
        
#**********
# start loop
       
propagateLabels(job).run()


#%% select hashtags

# outputs a list of possible hashtgs to add in ech camp satisfying 
# with
# count (= total number of occurrences),
# label_init (= initial label before propagation, -1 means no initial labels)
# vertex_id  (= ID of the vertex in the hashtag graph)
# label_sum1 (= number of neighbors with label 1)
# signi_sum1 (= sum of the significance of edges with neighbors having label 1)
# label_sum2 (= number of neighbors with label 2)
# signi_sum2 (= sum of the significance of edges with neighbors having label 2)
selectHashtags(job).run()

#%%
# you can now update the hashtag list and return the 1st step.
# lists of list of new hashtags (including initial hashtags)
job['htgs_lists'] = [['mortgage', 'rates', 'loan', 'loans', 'lenders', 'amortization', 'subprime'],
               ['etrade', 'tradeking', 'stock', '401k', 'market', 'ameritrade', 'scottrade']]

# loop end               
#******************

#%% update HT group in database
# `updateHTGroups` takes the lists of hashtags `htgs_lists` and mark then in 
# the database `sqlite_db_filename`.
#
# *Optional parameters that can be added to `job`:*
# - `column_name_ht_group` : name of the column added to the database (Default 
#   is `'ht_class'`). Different names can be used to test different `htgs_list`.

       
updateHTGroups(job).run()
               

#%% build training set
# `buildTrainingSet` reads tweets from the database with hashtags marked above,
# extract the features and labels of each tweets and saves them in 
# `features_pickle_file` and `labels_pickle_file`, respectively.
# Vectorized versions of the features and labels are saved to `features_vect_file` 
# and `labels_vect_file` for the cross-validation. A mapper between label names
# and label number is saved to `labels_mappers_file`.
#
# *Optional parameters:*
# - If the optional parameter `column_name_ht_group` has been changed in `job` 
#   in the step before, it will be used here to select the corresponding hashtag 
#   lists.
# - `undersample_maj_class` : whether to undersample the majority class in order
#   to balance the training set. Default is True, if False, unbalanced training 
#   set will be used and class weight will be adjusted accrodingly during training.
# (see http://scikit-learn.org/0.18/modules/generated/sklearn.linear_model.SGDClassifier.html) 

buildTrainingSet(job).run()
               

#%% Cross-Validation
# Estimate the performance of the classifier and optimize classifier parameters 
# with cross-validation. `crossValOptimize` loads the vectorized features and 
# labels (`features_vect_file` and `labels_vect_file`) and saves the results 
# of the optimization to `best_params_file` in JSON format.

# *Optional parameters:*
# - if `undersample_maj_class` was set to `False` when building the training set,
#   class weights will be adjusted to take into account different sizes of classes.
# - `ncpu` : number of cores to use (default is the number of cpus on your 
#   machine minus one).
# - `scoring` : The score used to optimize (default is `'f1_micro'`). 
#   for explanation and other possibilities. 
# - `n_splits` : number of folds (default is 10).
# - `loss` : loss function to be used. Default is `'log'` for Logistic Regression.
# - `penalty` : penalty of the regularization term (default is `'l2`).
# - `n_iter` : number of iterations of the gradient descent algorithm. 
#   Default is `5e5/(number of training samples)`. 
# - `grid_search_parameters` : parameter space to explore during the 
#   cross-validation. Default is 
#   `{'classifier__alpha' : np.logspace(-1,-7, num=20)}`, i.e. optimizing the 
#   regularization strength (`alpha`) between 1e-1 and 1e-7 with 20 
#   logarithmically spaced steps.
# - `verbose` : verbosity level of the calssifier (default is 1).

# See the sklearn Stochastic Gradient Descent user guide 
# (http://scikit-learn.org/0.18/modules/sgd.html#sgd) for recommended settings,
# the GridSearchCV (http://scikit-learn.org/0.18/modules/generated/sklearn.model_selection.GridSearchCV.html)
# and the Stochastic Gradient Descent documentations 
# (http://scikit-learn.org/0.18/modules/sgd.html#sgd) for details.

# here we use n_iter=2 for testing purposes
job['n_iter'] = 2
       
crossValOptimize(job).run()
               


#%% train classifier
# Uses features and labels from `features_pickle_file` and `labels_pickle_file`
# to train the classifier using the parameters from `best_params_file`.
# The trained classifier is then saved to `classifier_filename`.

trainClassifier(job).run()


#%% classify tweets
# Adds two tables `class_proba` and `retweet_class_proba` to the SQLite database
# with the result of the classification of each tweets and original retweeted status.
#
# *Optional parameters:*
# - `propa_table_name_suffix` : add a suffix to the two table names in order to
# compare different classifiers. Default is '' (empty string).

classifyTweets(job).run()

#%% make classification proba dataframe
# `makeProbaDF` reads the classification results from the database and processes 
# them to:
# - Replace the classification probability of retweets with the classification
#   results of the original tweets.
# - Replace the classification probability of tweets having a hashtag of one of
#   the two camps (and not of the other camp) with 0 (for camp1) or 1 (for camp2).
# - Discard tweets emanating from unoffical Twitter clients.
# 
# The results are saved as a pandas dataframe in `df_proba_filename`.
# 
# *Optional parameters:*
# - `use_official_clients` : whether you want to keep only tweets from official 
#   clients (`True`) or all tweets (`False`). Default is `True`.
# - `propa_table_name_suffix` can be changed to use the classification of 
#   different classifiers if it was used with `classifyTweets`.
# - `column_name_ht_group` is also used if it was changed to create a different 
#   training set.

       
makeProbaDF(job).run()


#%% analyze classification proba 
# `analyzeProbaDF` reads `df_proba_filename` and returns the number of tweets 
# and the number of users in each camp per day. The results are displayed and 
# saved as pandas dataframes to `df_num_tweets_filename` and `df_num_users_filename`.
#
# *Optional parameters:*
# - `ncpu` : number of cores to use. Default is number of cores of the machine 
# minus one.
# - `resampling_frequency` : frequency at which tweets are grouped. 
#   Default is `'D'`, i.e. daily. (see http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases) for different possibilities.)
# - `threshold` : threshold for the classifier probability (threshold >= 0.5).
#   Tweets with p > threshold are classified in camp2 and tweets with 
#   p < 1-threshold are classified in camp1. Default is 0.5.
# - `r_threshold` : threshold for the ratio of classified tweets needed to 
#   classify a user. Default is 0.5.

       
analyzeProbaDF(job).run()

