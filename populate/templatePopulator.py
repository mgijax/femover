#!/usr/local/bin/python
# 
# Template table populator for the Mover suite.
# (search for Template to find areas to change)

import sys
sys.path.insert (0, '../schema')

import Populator
import Template

###--- Classes ---###

class TemplatePopulator (Populator.Populator):
	# Is: a Populator for the 'Template' table in the MySQL database
	# Has: knowledge of how to refresh data for an individual Template key
	# Does: see Has (above) and comments on the Populator class itself

	def __init__ (self, tableInstance, inputFilename = None):
		# Purpose: see Populator.__init__()
		# Notes: In addition to the superclass constructor, this
		#	method also stores knowledge of which method to call
		#	to refresh data by the 'TemplateKey' field

		Populator.Populator.__init__ (self, tableInstance,
			inputFilename)
		self.setKeyMethods ( {
			'TemplateKey' : self.loadByTemplateKey,
			} )
		return

	def loadByTemplateKey (self,
		TemplateKey		# integer; refresh data for this key
		):
		# Purpose: to refresh the data for the specified key
		# Returns: nothing
		# Assumes: nothing
		# Modifies: updates the data in the Template table associated
		#	with the given Template key
		# Throws: propagates any exceptions from the load operation

		self.deleteByKey ('TemplateKey', TemplateKey)
		self.loadFile()
		return

###--- Globals ---###

# global instance of a TemplatePopulator
populator = TemplatePopulator (Template.table)

###--- main program ---###

# if invoked as a script, use the standard main() program for populators and
# pass in our particular populator
if __name__ == '__main__':
	Populator.main (populator)
