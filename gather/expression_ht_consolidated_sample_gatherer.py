#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_consolidated_sample' table in the front-end database

import Gatherer
import RNASeqUtils

###--- Classes ---###

class EHCSGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the expression_ht_consolidated_sample table
	# Has: queries to execute against the source database
	# Does: queries the source database for consolidated RNA-Seq samples,
	#	collates results, writes tab-delimited text file

	def collateResults(self):
		self.finalColumns = self.fieldOrder
		self.finalResults = RNASeqUtils.getConsolidatedSamples()
		return
	
###--- globals ---###

cmds = [ 'select 1' ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
    'consolidated_sample_key', 'experiment_key', 'genotype_key', 'organism', 'sex', 'age',
    'age_min', 'age_max', 'emapa_key', 'theiler_stage', 'note', 'sequence_num'
	]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_consolidated_sample'

# global instance of a EHCSGatherer
gatherer = EHCSGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
