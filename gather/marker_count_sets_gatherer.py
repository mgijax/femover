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

		# first two sets are special cases:  
		# 1) reagents

		(columns, rows) = self.results[0]
		keyCol = Gatherer.columnNumber (columns, MARKER_KEY)
		termCol = Gatherer.columnNumber (columns, 'term')
		countCol = Gatherer.columnNumber (columns, 'myCount')

		byMarker = {}			# byMarker[marker key] = {}

		nucleic = 'All nucleic'		# keys per marker in byMarker
		genomic = 'Genomic'
		cdna = 'cDNA'
		primerPair = 'Primer pair'
		other = 'Other'

		# collate counts per marker

		for row in rows:
			term = row[termCol]
			ct = row[countCol]
			key = row[keyCol]

			if not byMarker.has_key (key):
				byMarker[key] = {}

			if term == 'primer':
				byMarker[key][primerPair] = ct
			elif term == 'genomic':
				byMarker[key][genomic] = ct
			elif term == 'cDNA':
				byMarker[key][cdna] = ct
			else:
				byMarker[key][other] = ct

		# combine genomic, cDNA, and primer pair counts into nucleic

		for key in byMarker.keys():
			byMarker[key][nucleic] = 0
			for term in [ genomic, cdna, primerPair ]:
				if byMarker[key].has_key(term):
					byMarker[key][nucleic] = \
						byMarker[key][nucleic] + \
						byMarker[key][term]
			
		# generate rows, one per marker/count pair

		orderedTerms = [ nucleic, genomic, cdna, primerPair, other ]
		for key in byMarker.keys():
			for term in orderedTerms:
				if byMarker[key].has_key(term):
					i = i + 1
					newRow = [ key, 'Molecular reagents',
						term, byMarker[key][term], i ]
					self.finalResults.append (newRow)

		logger.debug ('finished set %s, %d rows so far' % (j, i) )
		j = j + 1

		# 2) polymorphisms

		(columns, rows) = self.results[1]
		keyCol = Gatherer.columnNumber (columns, MARKER_KEY)
		termCol = Gatherer.columnNumber (columns, 'term')
		countCol = Gatherer.columnNumber (columns, 'myCount')

		byMarker = {}			# byMarker[marker key] = {}

		all = 'All PCR and RFLP'	# keys per marker in byMarker
		pcr = 'PCR'
		rflp = 'RFLP'

		# collate counts per marker

		for row in rows:
			term = row[termCol]
			ct = row[countCol]
			key = row[keyCol]

			if not byMarker.has_key (key):
				byMarker[key] = {}

			if term == 'primer':
				byMarker[key][pcr] = ct
			elif not byMarker[key].has_key(rflp):
				byMarker[key][rflp] = ct
			else:
				byMarker[key][rflp] = byMarker[key][rflp] + ct

		# if we had both PCR and RFLP then we need a count for the sum

		for key in byMarker.keys():
			if len(byMarker[key]) > 1:
				byMarker[key][all] = byMarker[key][pcr] + \
					byMarker[key][rflp]
			
		# generate rows, one per marker/count pair

		orderedTerms = [ all, pcr, rflp ]
		for key in byMarker.keys():
			for term in orderedTerms:
				if byMarker[key].has_key(term):
					i = i + 1
					newRow = [ key, 'Polymorphisms',
						term, byMarker[key][term], i ]
					self.finalResults.append (newRow)

		logger.debug ('finished set %s, %d rows so far' % (j, i) )
		j = j + 1

		# the remaining sets are have standard format and can be done
		# in a nested loop

		for (columns, rows) in self.results[2:]:
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
	# counts of reagents by type (these need to be grouped in code, but
	# this will give us the raw counts)
	'''select pm._Marker_key,
		vt.term,
		count(distinct pp._Probe_key) as myCount
	from prb_marker pm,
		prb_probe pp,
		voc_term vt
	where pp._SegmentType_key = vt._Term_key
		and pm._Probe_key = pp._Probe_key
	group by pm._Marker_key, vt.term''',

	# counts of RFLP/PCR polymorphisms by type
	'''select rflv._Marker_key,
		t.term,
		count(rflv._Reference_key) as myCount
	from prb_probe p,
		prb_rflv rflv,
		prb_reference r,
		voc_term t
	where p._SegmentType_key = t._Term_key
		and rflv._Reference_key = r._Reference_key
		and r._Probe_key = p._Probe_key
	group by rflv._Marker_key, t.term''',

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
