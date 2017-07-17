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


tweet_archive_dirs = ['etrade']
sqlite_file = 'test.sqlite'
graph_file = 'graph_file.graphml'

features_pickle_file = 'features.pickle'
labels_pickle_file = 'labels.pickle'
features_vect_file = 'features.mmap'
labels_vect_file = 'labels.mmap'
labels_mappers_file = 'labels_mappers.pickle'
classifier_filename = 'classifier.pickle'
propag_results_filename = 'propag_results.pickle'
df_proba_filename = 'df_proba.pickle'
df_num_tweets_filename = 'df_num_tweets.pickle'
df_num_users_filename = 'df_num_users.pickle'
best_params_file = 'best_params.json'


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

#%% build database

bDB = buildDatabse(job)

r = bDB.run()

#%% make HT network

mHTn = makeHTNetwork(job)

mHTn.run()

#%% add significance value to edges

aSSHT = addStatSigniHT(job)

aSSHT.run()

#%% select initial hashtags
       
sIHT = selectInitialHashtags(job)
       
sIHT.run()
# user input
initial_htgs_lists = [['money'],
                      ['401k']]
                      

#%% propagate labels
# start loop
job['initial_htgs_lists'] = initial_htgs_lists
       
pL = propagateLabels(job)

pL.run()

#%% select hashtags
        
sHT = selectHashtags(job)

sHT.run()

# lists of list of new hashtags (including initial hashtags)
htgs_lists = [['money', 'stocks', 'stockmarket', 'cash', 'market', 'ameritrade', 'scottrade'],
               ['401k', 'finance', 'etrade', 'tradeking', 'stock', 'amtd', 'alerts']]
# loop end               
# these two last step can be looped using htgs_lists as initial_htgs_lists until
# a satisfying set of hashtags is found
#%% update HT group in database

job['htgs_lists'] = htgs_lists
       
uHTG = updateHTGroups(job)
               
uHTG.run()

#%% build training set

       
bTS = buildTrainingSet(job)
               
bTS.run()

#%% optimize
job['n_iter'] = 2
       
CV = crossValOptimize(job)
               
CV.run()


#%% train classifier


tC = trainClassifier(job)

tC.run()

#%% classify tweets

CT = classifyTweets(job)

CT.run() 
#%% make classification proba dataframe
       
mPDF = makeProbaDF(job)

mPDF.run() 

#%% analyze classification proba 
       
aPDF = analyzeProbaDF(job)

aPDF.run()

print(aPDF.string_results)

