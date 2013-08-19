#!/usr/local/bin/python
# 
# gathers data for the 'marker_searchable_nomenclature' table in the front-end
# database

import Gatherer

###--- Functions ---###

def stripBrackets (s):
	return s.replace('<', '').replace('>', '')

###--- Classes ---###

class MarkerNomenGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_searchable_nomenclature table
	# Has: queries to execute against the source database
	# Does: queries the source database for searchable nomencalture terms
	#	for markers, collates results, writes tab-delimited text file

	def collateResults (self):
		self.finalColumns = [ '_Marker_key', 'term', 'term_type',
			'sequence_num' ]
		self.finalResults = []

		cols, rows = self.results[0]

		termCol = Gatherer.columnNumber (cols, 'term')
		typeCol = Gatherer.columnNumber (cols, 'term_type')
		keyCol = Gatherer.columnNumber (cols, '_Marker_key')

		seqNum = 0

		for row in rows:
			seqNum = seqNum + 1
			self.finalResults.append ( [ row[keyCol],
				row[termCol], row[typeCol], seqNum ] )
		return

###--- globals ---###

cmds = [
	# 0. variety of items already cached in MRK_Label, including...
	#	a. current marker symbol
	#	b. current marker name
	#	c. old symbols
	#	d. old names
	#	e. synonyms
	#	f. related synonyms
	#	g. allele symbols
	#	h. allele names
	#	i. human ortholog symbol
	#	j. rat ortholog symbol
	#	k. all other ortholog symbols
	#	l. human ortholog name
	#	m. human synonyms
	#	n. rat synonyms
	'''select ml.label as term,
			ml.labelTypeName as term_type,
			mm._Marker_key,
			ml.priority
		from mrk_marker mm,
			mrk_label ml
		where mm._Organism_key = 1
			and mm._Marker_Status_key in (1,3)
			and mm._Marker_key = ml._Marker_key
			and ml.labeltypename not in ('allele symbol','allele name')
		order by mm._Marker_key, ml.priority, ml.labelTypeName''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', 'term', 'term_type', 'sequence_num',
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_searchable_nomenclature'

# global instance of a MarkerNomenGatherer
gatherer = MarkerNomenGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
