#!/usr/local/bin/python
# 
# gathers data for the 'marker_count_sets' table in the front-end database

import Gatherer
import logger

###--- Constants ---###

MARKER_KEY = '_Marker_key'
SET_TYPE = 'setType'
COUNT_TYPE = 'countType'
COUNT = 'count'
SEQUENCE_NUM = 'sequenceNum'

###--- Classes ---###

class MarkerCountSetsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_count_sets table
	# Has: queries to execute against the source database
	# Does: queries the source database for counts for sets of items
	#	related to a marker (like counts of alleles by type),
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# combine the result sets from the various queries into a
		# single set of final results

		self.finalColumns = [ MARKER_KEY, SET_TYPE, COUNT_TYPE,
			COUNT, SEQUENCE_NUM ]
		self.finalResults = []

		# we need to store an ordering for the items which ensures
		# that the counts for a various set of a various marker are
		# ordered correctly.  This does not require starting the order
		# for each set at 1, so just use a common ascending counter.

		i = 0		# counter for ordering of rows
		j = 0		# counter of result sets

		for (columns, rows) in self.results:
			keyCol = Gatherer.columnNumber (columns, MARKER_KEY)
			setCol = Gatherer.columnNumber (columns, SET_TYPE)
			typeCol = Gatherer.columnNumber (columns, COUNT_TYPE)
			countCol = Gatherer.columnNumber (columns, COUNT)

			for row in rows:
				i = i + 1
				newRow = [ row[keyCol], row[setCol],
					row[typeCol], row[countCol], i ]
				self.finalResults.append (newRow)

			logger.debug ('finished set %s, %d rows so far' % (
				j, i) )
			j = j + 1
		return

###--- globals ---###

cmds = [
	# alleles by type (and these aren't the actual types, but the
	# groupings of types defined as vocabulary associations)
	'''select a._Marker_key,
		vt.term as %s,
		'Alleles' as %s,
		count(1) as %s,
		vt.sequenceNum
	from all_allele a,
		mgi_vocassociationtype mvat,
		mgi_vocassociation mva,
		voc_term vt
	where mvat.associationType = 'Marker Detail Allele Category'
		and mvat._AssociationType_key = mva._AssociationType_key
		and mva._Term_key_1 = vt._Term_key
		and mva._Term_key_2 = a._Allele_Type_key
		and a.isWildType = 0
		and a._Marker_key is not null
	group by a._Marker_key, vt.term, vt.sequenceNum
	order by a._Marker_key, vt.sequenceNum''' % (COUNT_TYPE, SET_TYPE,
		COUNT),

	# expression assays by type
	'''select ge._Marker_key,
		gat.assayType as %s,
		'Expression Assays by Assay Type' as %s,
		count(distinct ge._Assay_key) as %s
	from gxd_expression ge,
		gxd_assaytype gat
	where ge._AssayType_key = gat._AssayType_key
		and ge.isForGXD = 1
	group by ge._Marker_key, gat.assayType
	order by ge._Marker_key, gat.assayType''' % (COUNT_TYPE, SET_TYPE,
		COUNT),

	# expression results by type
	'''select ge._Marker_key,
		gat.assayType as %s,
		'Expression Results by Assay Type' as %s,
		count(distinct ge._Expression_key) as %s
	from gxd_expression ge,
		gxd_assaytype gat
	where ge._AssayType_key = gat._AssayType_key
		and ge.isForGXD = 1
	group by ge._Marker_key, gat.assayType
	order by ge._Marker_key, gat.assayType''' % (COUNT_TYPE, SET_TYPE,
		COUNT),

	# expression results by theiler stages 
	'''select ge._Marker_key,
		ts.stage as %s,
		'Expression Results by Theiler Stage' as %s,
		count(distinct ge._Expression_key) as %s
	from gxd_expression ge,
		gxd_structure gs,
		gxd_theilerstage ts
	where ge.isForGXD = 1
		and ge._Structure_key = gs._Structure_key
		and gs._Stage_key = ts._Stage_key
	group by ge._Marker_key, ts.stage
	order by ge._Marker_key, ts.stage''' % (COUNT_TYPE, SET_TYPE, COUNT),
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, MARKER_KEY, SET_TYPE, COUNT_TYPE, COUNT,
	SEQUENCE_NUM ]

# prefix for the filename of the output file
filenamePrefix = 'marker_count_sets'

# global instance of a MarkerCountSetsGatherer
gatherer = MarkerCountSetsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
