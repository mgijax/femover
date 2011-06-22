#!/usr/local/bin/python
# 
# gathers data for the 'batch_marker_go_annotations' table in the front-end
# database

import Gatherer

###--- Classes ---###

class BatchMarkerGOGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the batch_marker_go_annotations table
	# Has: queries to execute against the source database
	# Does: queries the source database for a tiny subset of GO annotation
	#	data that we need for batch query results, collates results,
	#	writes tab-delimited text file

	def postprocessResults (self):
		self.convertFinalResultsToList()

		i = 1
		for row in self.finalResults:
			self.addColumn ('sequence_num', i, row,
				self.finalColumns)
			i = i + 1
		return

###--- globals ---###

cmds = [
	'''select distinct an._Object_key as _Marker_key,
		ag.accID as go_id,
		vt.term as term,
		evt.abbreviation as evidence_code
	from voc_annot an
	left outer join acc_accession ag on (an._Term_key = ag._Object_key)
	left outer join voc_term vt on (an._Term_key = vt._Term_key)
	left outer join voc_evidence ev on (an._Annot_key = ev._Annot_key)
	left outer join voc_term evt on (ev._EvidenceTerm_key = evt._Term_key)
	where an._AnnotType_key = 1000
		and an._Qualifier_key != 1614151
		and vt._Vocab_key = 4
		and ag._LogicalDB_key = 31
		and ag._MGIType_key = 13
		and ag.preferred = 1
	order by _Marker_key, term, evidence_code''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', 'go_id', 'term', 'evidence_code',
	'sequence_num'
	]

# prefix for the filename of the output file
filenamePrefix = 'batch_marker_go_annotations'

# global instance of a BatchMarkerGOGatherer
gatherer = BatchMarkerGOGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
