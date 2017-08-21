"""
	Base class for modules
"""
import time

class BaseModule():
    def __init__(self, job):
        self.job = job

    
    def print_elapsed_time(t0):
            
        print('*** took ' + "{:.4}".format(time.time()-t0) + 's')                

