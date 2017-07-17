"""
	this is the main 
"""

"""
	define classes in this section
	classdict uses job['fct'] to determine which class to use
"""

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

from mydsclass import myDSClass

classdict = {
	'foo':myDSClass,
	'buildDatabase':buildDatabse,
	'makeHTnetwork':makeHTNetwork,
	'selectInitialHashtags':selectInitialHashtags,
	'propagateLabels':propagateLabels,
	'addStatSigniHT':addStatSigniHT,
	'selectHashtags':selectHashtags,
	'updateHTGroups':updateHTGroups,
	'buildTrainingSet':buildTrainingSet,
	'crossValOptimize':crossValOptimize,
	'trainClassifier':trainClassifier,
	'classifyTweets':classifyTweets,
	'makeProbaDF':makeProbaDF,
	'analyzeProbaDF':analyzeProbaDF	
}

"""
	no need to touch code below
"""

from job import Job
		
def clbk(job):
	# This is called when a msg is received by the exchange
	fct = job['fct']
	D = classdict[fct](job,J.reportResult)
	D.run()
	
try:
	J = Job(clbk)
	J.run()
	
except KeyboardInterrupt:
	print("Stopping")
	J.stop()