#!/usr/local/bin/python
# 
# gathers data for the 'batch_marker_mp_annotations' table in the front-end
# database

import Gatherer

###--- Classes ---###

class BatchMarkerMPGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the batch_marker_mp_annotations table
	# Has: queries to execute against the source database
	# Does: queries the source database for a tiny subset of MP annotation
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
	# 0. set of MP annotations that have been rolled up to markers (only
	# keep null qualifiers, to avoid NOT and "normal" annotations).
	'''select distinct va._Object_key as _Marker_key,
		a.accID as mp_id,
		t.term
	from voc_annot va,
		voc_term t,
		acc_accession a,
		voc_term q
	where va._AnnotType_key = 1015
		and va._Term_key = t._Term_key
		and va._Term_key = a._Object_key
		and a._MGIType_key = 13
		and a.preferred = 1
		and va._Qualifier_key = q._Term_key
		and q.term is null
	order by va._Object_key, a.accID''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', 'mp_id', 'term', 'sequence_num'
	]

# prefix for the filename of the output file
filenamePrefix = 'batch_marker_mp_annotations'

# global instance of a BatchMarkerMPGatherer
gatherer = BatchMarkerMPGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
