#!/usr/local/bin/python

# This script is responsible for keeping the object mapping for a reference.  
# It can be invoked in one of two ways, either by simply calling it
# which will in turn call all of the gather/populate scripts for a reference.

import Controller

###--- Globals ---###

# list of MySQL tables to be updated
tables = [ 'reference', 'referenceID', 'referenceBook', 'referenceAbstract',
	'referenceCounts', 'referenceSequenceNum', ]

# global Controller object for the reference tables
controller = Controller.Controller (tables)

###--- main program ---###

# if invoked as a script, use the standard main() program for Controllers,
# passing in our particular controller
if __name__ == '__main__':
	Controller.main (controller)
