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
	'''select distinct al._Marker_key,
		ac.accID as mp_id,
		mp.term as term
	from all_allele al, gxd_allelegenotype gag, voc_term mp,
		voc_annot ova, acc_accession ac
	where al._Allele_key = gag._Allele_key
		and gag._Genotype_key = ova._Object_key
		and ova._Term_key = mp._Term_key
		and ova._AnnotType_key = 1002
		and ova._Qualifier_key != 2181424
		and ova._Term_key = ac._Object_key
		and ac._MGIType_key = 13
		and ac.preferred = 1
		and ac.prefixPart = 'MP:'
		and al._Marker_key is not null
	order by _Marker_key, mp_id'''
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
