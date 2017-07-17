"""
	no need to change this file
"""

class DS():
	def __init__(self,job):
		self.job = job

#	def repportBack(self,taskSize,completedSize,result,error,warning):
#		obj={}
#		
#		obj['jobid']=self.jobid
#		obj['request']=self.job
#		obj['taskSize'] = taskSize
#		obj['completedSize']=completedSize
#		
#		if(result):
#			obj['result']=result
#		if(error):
#			obj['error']=1
#			obj['errormessage']=error
#		if(warning):
#			obj['warning']=1
#			obj['warningmessage']=warning
#
#		self.clbk(obj)
		
#	def readData(self,bucket,key):
#		return self.s3.readFile(bucket,key)
#	def readCSVData(self,bucket,key):
#		return self.s3.readCSVFile(bucket,key)		
#		
#	def saveData(self,bucket,key,data):
#		return self.s3.saveData(bucket,key,data)
		
