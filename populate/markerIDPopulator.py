#!/usr/local/bin/python
# 
# markerID table populator for the Mover suite.

import sys
sys.path.insert (0, '../schema')

import Populator
import markerID

###--- Classes ---###

class MarkerIDPopulator (Populator.Populator):
	# Is: a Populator for the 'markerID' table in the MySQL database
	# Has: knowledge of how to refresh data for an individual marker key
	# Does: see Has (above) and comments on the Populator class itself

	def __init__ (self, tableInstance, inputFilename = None):
		# Purpose: see Populator.__init__()
		# Notes: In addition to the superclass constructor, this
		#	method also stores knowledge of which method to call
		#	to refresh data by the 'markerKey' field

		Populator.Populator.__init__ (self, tableInstance,
			inputFilename)
		self.setKeyMethods ( {
			'markerKey' : self.loadByMarkerKey,
			} )
		return

	def loadByMarkerKey (self,
		markerKey		# integer; refresh data for this key
		):
		# Purpose: to refresh the data for the specified key
		# Returns: nothing
		# Assumes: nothing
		# Modifies: updates the data in the markerID table associated
		#	with the given markerKey
		# Throws: propagates any exceptions from the load operation

		self.deleteByKey ('markerKey', markerKey)
		self.loadFile()
		return

###--- Globals ---###

# global instance of a MarkerIDPopulator
populator = MarkerIDPopulator (markerID.table)

###--- main program ---###

# if invoked as a script, use the standard main() program for populators and
# pass in our particular populator
if __name__ == '__main__':
	Populator.main (populator)
