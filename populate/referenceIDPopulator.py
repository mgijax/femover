#!/usr/local/bin/python
# 
# referenceID table populator for the Mover suite.

import sys
sys.path.insert (0, '../schema')

import Populator
import referenceID

###--- Classes ---###

class ReferenceIDPopulator (Populator.Populator):
	# Is: a Populator for the 'referenceID' table in the MySQL database
	# Has: knowledge of how to refresh data for an individual reference
	#	key
	# Does: see Has (above) and comments on the Populator class itself

	def __init__ (self, tableInstance, inputFilename = None):
		# Purpose: see Populator.__init__()
		# Notes: In addition to the superclass constructor, this
		#	method also stores knowledge of which method to call
		#	to refresh data by the 'referenceKey' field

		Populator.Populator.__init__ (self, tableInstance,
			inputFilename)
		self.setKeyMethods ( {
			'referenceKey' : self.loadByReferenceKey,
			} )
		return

	def loadByReferenceKey (self,
		referenceKey		# integer; refresh data for this key
		):
		# Purpose: to refresh the data for the specified key
		# Returns: nothing
		# Assumes: nothing
		# Modifies: updates the data in the referenceID table
		#	associated with the given reference key
		# Throws: propagates any exceptions from the load operation

		self.deleteByKey ('referenceKey', referenceKey)
		self.loadFile()
		return

###--- Globals ---###

# global instance of a ReferenceIDPopulator
populator = ReferenceIDPopulator (referenceID.table)

###--- main program ---###

# if invoked as a script, use the standard main() program for populators and
# pass in our particular populator
if __name__ == '__main__':
	Populator.main (populator)
