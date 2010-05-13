#!/usr/local/bin/python
# 
# alleleAnnotation table populator for the Mover suite.

import sys
sys.path.insert (0, '../schema')

import Populator
import alleleAnnotation

###--- Classes ---###

class AlleleAnnotationPopulator (Populator.Populator):
	# Is: a Populator for the 'alleleAnnotation' table in the MySQL
	#	database
	# Has: knowledge of how to refresh data for an individual allele key
	# Does: see Has (above) and comments on the Populator class itself

	def __init__ (self, tableInstance, inputFilename = None):
		# Purpose: see Populator.__init__()
		# Notes: In addition to the superclass constructor, this
		#	method also stores knowledge of which method to call
		#	to refresh data by the 'alleleKey' field

		Populator.Populator.__init__ (self, tableInstance,
			inputFilename)
		self.setKeyMethods ( {
			'alleleKey' : self.loadByAlleleKey,
			} )
		return

	def loadByAlleleKey (self,
		alleleKey		# integer; refresh data for this key
		):
		# Purpose: to refresh the data for the specified key
		# Returns: nothing
		# Assumes: nothing
		# Modifies: updates the data in the alleleAnnotation table
		#	associated with the given allele key
		# Throws: propagates any exceptions from the load operation

		self.deleteByKey ('alleleKey', alleleKey)
		self.loadFile()
		return

###--- Globals ---###

# global instance of a AlleleAnnotationPopulator
populator = AlleleAnnotationPopulator (alleleAnnotation.table)

###--- main program ---###

# if invoked as a script, use the standard main() program for populators and
# pass in our particular populator
if __name__ == '__main__':
	Populator.main (populator)
