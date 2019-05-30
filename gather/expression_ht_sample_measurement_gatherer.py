#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_sample_measurement' table in the front-end database

import Gatherer
import RNASeqUtils

###--- Classes ---###

class EHSMGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the expression_ht_sample_measurement table
	# Has: queries to execute against the source database
	# Does: queries the source database for measurements for RNA-Seq samples,
	#	collates results, writes tab-delimited text file

	def collateResults(self):
		self.finalColumns = self.fieldOrder
		self.finalResults = RNASeqUtils.getSampleMeasurements()
		return
	
###--- globals ---###

cmds = [ 'select 1' ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
    'measurement_key', 'sample_key', 'marker_key', 'marker_id', 'marker_symbol', 'average_tpm', 'qn_tpm'
	]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_sample_measurement'

# global instance of a TemplateGatherer
gatherer = EHSMGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
