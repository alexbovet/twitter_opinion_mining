"""
	Base class for modules
"""

# Author: Alexandre Bovet <alexandre.bovet@gmail.com>
# License: BSD 3 clause

import time

class baseModule():
    def __init__(self, job):
        self.job = job

    @staticmethod
    def print_elapsed_time(t0):
            
        print('*** took ' + "{:.4}".format(time.time()-t0) + 's')                

