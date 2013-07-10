#!/usr/local/bin/python
# 
# 	Gathers data for the glossary index and term pages
#	Reads the glossary.rcd file from the data directory
#
#	Author: kstone 2013/07/10
#

import Gatherer
import logger
import dbAgnostic
import rcdlib
import config

###--- Classes ---###
class GlossaryGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the glossary_term table
	def readRcd(self):
		filepath = config.GLOSSARY_FILE
		rcdFile = rcdlib.RcdFile (filepath, rcdlib.Rcd, 'key')
		return rcdFile

	def collateResults(self):
		cols = ['unique_key','glossary_key','display_name','definition']
		rcdFile = self.readRcd()

		count = 0
		dbrows = []	
		count = 0
		for key,rcd in rcdFile.items():
			count += 1
			definition = rcd["definition"]
			definition = definition.replace("definition=","")
			dbrows.append((count,key,rcd["displayName"],rcd["definition"]))
			
		self.finalColumns = cols
		self.finalResults = dbrows
		return

	def postprocessResults(self):
		return

###--- globals ---###

# The gatherer class becomes sad when there are no queries to execute at creation time
# This query won't actually be used by this code
# TODO: please modify if you know how to fix this without the need for my hack
cmds = ["""select 1 from mrk_marker limit 1"""] 

# order of fields (from the query results) to be written to the
# output file
fieldOrder = ['unique_key','glossary_key','display_name','definition']

# prefix for the filename of the output file
filenamePrefix = 'glossary_term'

# global instance of a AssayGatherer
gatherer = GlossaryGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
