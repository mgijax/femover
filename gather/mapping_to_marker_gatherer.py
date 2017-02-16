#!/usr/local/bin/python
# 
# gathers data for the 'mapping_to_marker' table in the front-end database

import Gatherer

###--- Classes ---###

MappingToMarkerGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the mapping_to_marker table
	# Has: queries to execute against the source database
	# Does: queries the source database for relationships between mapping experiments and markers,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select e._Expt_key, e._Marker_key, e._Allele_key, a.description as assay_type, e.description,
			e.sequenceNum
		from MLD_Expt_Marker e, MLD_Expts me, MLD_Assay_Types a
		where e._Assay_Type_key = a._Assay_Type_key
			and e._Expt_key = me._Expt_key
			and me.exptType != 'CONTIG' ''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Expt_key', '_Marker_key', '_Allele_key', 'assay_type', 'description', 'sequenceNum', ]

# prefix for the filename of the output file
filenamePrefix = 'mapping_to_marker'

# global instance of a MappingToMarkerGatherer
gatherer = MappingToMarkerGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
