#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_sample_map' table in the front-end database

import Gatherer

###--- Classes ---###

EHSMGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the expression_ht_sample_map table
	# Has: queries to execute against the source database
	# Does: queries the source database for relationships between samples and their consolidated versions,
	#	collates results, writes tab-delimited text file
	
###--- globals ---###

cmds = [ '''select _RNASeqSet_key, _Sample_key, row_number() over (order by _RNASeqSet_key, _Sample_key) as sequence_num
	from gxd_htsample_rnaseqsetmember s
	order by 3'''
]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_RNASeqSet_key', '_Sample_key', 'sequence_num',
	]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_sample_map'

# global instance of a TemplateGatherer
gatherer = EHSMGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
