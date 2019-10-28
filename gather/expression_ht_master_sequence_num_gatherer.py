#!/usr/local/bin/python
# 
# Gathers a minimal set of data (1 row) for the 'expression_ht_master_sequence_num' table in the front-end database.
# This is just enough to have the table created, then we actually delete its one row, then compute and add its real contents in a 
# post-processing script.

import Gatherer

SeqNumGatherer = Gatherer.Gatherer

cmds = [
	'''select 1 as uniqueKey, 0 as isClassical, c._RNASeqCombined_key, 
		1 as byGeneSymbol, 1 as byAge, 1 as byStructure,
		1 as byExpressed, 1 as byExperiment
	    from gxd_htsample_rnaseqcombined c
	    limit 1'''
	]

# order of fields (from the query results) to be written to the output file
fieldOrder = [ 'uniqueKey', 'isClassical', '_RNASeqCombined_key', 'byGeneSymbol', 'byAge', 'byStructure', 'byExpressed', 'byExperiment' ]

filenamePrefix = 'expression_ht_master_sequence_num'

# global instance of a TemplateGatherer
gatherer = SeqNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
