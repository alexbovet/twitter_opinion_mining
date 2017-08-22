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

bDB = buildDatabse(job)

bDB.run()

#%% make HT network

makeHTNetwork(job).run()


#%% add significance value to edges

addStatSigniHT(job).run()


#%% select initial hashtags
       
# this will display the top occuring hashtags
selectInitialHashtags(job).run()

#%%
# select the seeds you want to use for the label propagation
# user input
initial_htgs_lists = [['money'],
                      ['401k']]
                      

#%% propagate labels
# start loop
job['initial_htgs_lists'] = initial_htgs_lists
       
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
# lists of list of new hashtags (including initial hashtags)
htgs_lists = [['money', 'stocks', 'stockmarket', 'cash', 'market', 'ameritrade', 'scottrade'],
               ['401k', 'finance', 'etrade', 'tradeking', 'stock', 'amtd', 'alerts']]
# loop end               
# these two last step can be looped using htgs_lists as initial_htgs_lists until
# a satisfying set of hashtags is found
#%% update HT group in database

job['htgs_lists'] = htgs_lists
       
updateHTGroups(job).run()
               

#%% build training set

buildTrainingSet(job).run()
               

#%% optimize
job['n_iter'] = 2
       
crossValOptimize(job).run()
               


#%% train classifier

trainClassifier(job).run()


#%% classify tweets

classifyTweets(job).run()

#%% make classification proba dataframe
       
makeProbaDF(job).run()


#%% analyze classification proba 
       
analyzeProbaDF(job).run()

