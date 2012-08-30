#!/usr/local/bin/python
# 
# gathers data for the marker_to_expression_assay table in the front-end db

import Gatherer

###--- Classes ---###

MarkerToAssayGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the marker_to_expression_assay table
	# Has: queries to execute against the source database
	# Does: queries the source database for relationships between markers
	#	and expression assays,collates results, writes tab-delimited
	#	text file

###--- globals ---###

cmds = [ '''select a._Marker_key,
			a._Assay_key,
			a._Refs_key,
			null as qualifier
		from gxd_assay a
		where exists (select 1 from gxd_expression e where a._Assay_key = e._Assay_key)''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Assay_key', '_Refs_key', 'qualifier'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_to_expression_assay'

# global instance of a MarkerToAssayGatherer
gatherer = MarkerToAssayGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
