#!/usr/local/bin/python
# 
# gathers data for the 'marker_count_sets' table in the front-end database
#
# 01/21/2014	lec
#	- TR11515/allele type changes
#


import Gatherer
import logger
import MarkerSnpAssociations
import ADMapper
import InteractionUtils
import MarkerUtils
import OutputFile
import gc

###--- Constants ---###

MARKER_KEY = '_Marker_key'
STRUCTURE_KEY = '_Structure_key'
SET_TYPE = 'setType'
COUNT_TYPE = 'countType'
COUNT = 'count'
SEQUENCE_NUM = 'sequenceNum'

ASSAY_TYPE_ORDER = [
	'Immunohistochemistry',
	'RNA in situ',
	'In situ reporter (knock in)',
	'Northern blot',
	'Western blot',
	'RT-PCR',
	'RNase protection',
	'Nuclease S1',
	]

###--- Functions ---###

def stageCompare (a, b):
	return cmp(int(a), int(b))

def assayTypeSortVal (assayType):
	global ASSAY_TYPE_ORDER

	if assayType in ASSAY_TYPE_ORDER:
		return ASSAY_TYPE_ORDER.index(assayType)

	ASSAY_TYPE_ORDER.append(assayType)
	return ASSAY_TYPE_ORDER.index(assayType)

def assayTypeCompare (a, b):
	return cmp(assayTypeSortVal(a), assayTypeSortVal(b))

###--- Classes ---###

class MarkerCountSetsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_count_sets table
	# Has: queries to execute against the source database
	# Does: queries the source database for counts for sets of items
	#	related to a marker (like counts of alleles by type),
	#	collates results, writes tab-delimited text file

	def report (self):
		logger.debug ('finished set %d, %d rows so far' % (
			self.j, self.i))
		self.j = self.j + 1
		return

	def seqNum (self):
		self.i = self.i + 1
		return self.i

	def collateResultsByTheilerStage (self, resultIndex):
		# expression results per Theiler stage (now custom, because
		# we need to translate from AD structures to EMAPS terms)

		setType = 'Expression Results by Theiler Stage'

		cols, rows = self.results[resultIndex]

		markerCol = Gatherer.columnNumber (cols, MARKER_KEY)
		structureCol = Gatherer.columnNumber (cols, STRUCTURE_KEY)
		countCol = Gatherer.columnNumber (cols, COUNT)

		rows = ADMapper.filterRows (rows, structureCol, setType)

		# first we'll translate each structure to its equivalent EMAPS
		# term, then we'll find the stage from that EMAPS term.

		# resultCounts[markerKey] = { stage : count of results }
		resultCounts = {}

		for row in rows:
			markerKey = row[markerCol]
			structureKey = row[structureCol]
			resultCount = row[countCol]

			emapsKey = ADMapper.getEmapsKey(structureKey)
			stage = ADMapper.getStageByKey(emapsKey)
			if not stage:
				continue

			# strip any leading zero
			if stage[0] == '0':
				stage = stage[1:]

			if not resultCounts.has_key(markerKey):
				resultCounts[markerKey] = {
					stage : resultCount }

			elif not resultCounts[markerKey].has_key(stage):
				resultCounts[markerKey][stage] = resultCount

			else:
				resultCounts[markerKey][stage] = resultCount \
					+ resultCounts[markerKey][stage]

		# free up memory for result set, once we've walked through it
		self.results[resultIndex] = (cols, [])
		del rows
		gc.collect()

		# At this point, we have collected our counts of expression
		# results by marker and Theiler stage.  Time to assemble rows
		# from those data.

		markerKeys = resultCounts.keys()
		markerKeys.sort()

		for markerKey in markerKeys:
			stages = resultCounts[markerKey].keys()
			stages.sort(stageCompare)

			for stage in stages:
				newRow = [ markerKey, setType, stage,
					resultCounts[markerKey][stage],
					self.seqNum() ]
				self.finalResults.append (newRow)
		self.report()
		self.writeSoFar()
		return

	def collateByAssayType (self, resultIndex, setType):
		# expression assays or results per Theiler stage (now custom,
		# because we need to filter out AD structures that do not map
		# to EMAPS terms)

		cols, rows = self.results[resultIndex]

		markerCol = Gatherer.columnNumber (cols, MARKER_KEY)
		structureCol = Gatherer.columnNumber (cols, STRUCTURE_KEY)
		countCol = Gatherer.columnNumber (cols, COUNT)
		assayTypeCol = Gatherer.columnNumber (cols, COUNT_TYPE)

		# filter out any associations to AD structures which cannot be
		# mapped to EMAPS terms

		rows = ADMapper.filterRows (rows, structureCol, setType)

		# resultCounts[markerKey] = { assay type : count of results }
		resultCounts = {}

		for row in rows:
			markerKey = row[markerCol]
			count = row[countCol]
			assayType = row[assayTypeCol]

			if not resultCounts.has_key(markerKey):
				resultCounts[markerKey] = { assayType : count }

			elif not resultCounts[markerKey].has_key(assayType):
				resultCounts[markerKey][assayType] = count

			else:
				resultCounts[markerKey][assayType] = count \
					+ resultCounts[markerKey][assayType]

		# free up memory for result set, once we've walked through it
		self.results[resultIndex] = (cols, [])
		del rows
		gc.collect()

		# At this point, we have collected our counts of expression
		# results by marker and Theiler stage.  Time to assemble rows
		# from those data.

		markerKeys = resultCounts.keys()
		markerKeys.sort()

		for markerKey in markerKeys:
			assayTypes = resultCounts[markerKey].keys()
			assayTypes.sort(assayTypeCompare)

			for assayType in assayTypes:
				newRow = [ markerKey, setType, assayType,
					resultCounts[markerKey][assayType],
					self.seqNum() ]
				self.finalResults.append (newRow)
		self.report()
		self.writeSoFar()
		return

	def collateResultsByAssayType (self, resultIndex):
		setType = 'Expression Results by Assay Type'
		self.collateByAssayType (resultIndex, setType)
		return

	def collateAssaysByAssayType (self, resultIndex):
		# expression results per Theiler stage (now custom, because
		# we need to translate from AD structures to EMAPS terms)

		setType = 'Expression Assays by Assay Type'
		self.collateByAssayType (resultIndex, setType)
		return

	def collateReagents (self, resultIndex):
		(columns, rows) = self.results[resultIndex]
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

		# free up memory for result set, once we've walked through it
		self.results[resultIndex] = (columns, [])
		del rows
		gc.collect()

		# combine genomic, cDNA, primer pair, other into nucleic

		for key in byMarker.keys():
			byMarker[key][nucleic] = 0
			for term in [ genomic, cdna, primerPair, other ]:
				if byMarker[key].has_key(term):
					byMarker[key][nucleic] = \
						byMarker[key][nucleic] + \
						byMarker[key][term]
			
		# generate rows, one per marker/count pair

		orderedTerms = [ nucleic, genomic, cdna, primerPair, other ]
		for key in byMarker.keys():
			for term in orderedTerms:
				if byMarker[key].has_key(term):
					newRow = [ key, 'Molecular reagents',
						term, byMarker[key][term],
						self.seqNum() ]
					self.finalResults.append (newRow)
		self.report()
		self.writeSoFar()
		return

	def collatePolymorphisms (self, resultIndex):
		logger.debug ('Retrieving SNP data')
		markersWithSNPs = MarkerSnpAssociations.getAllMarkerCounts()
		logger.debug ('Found %d markers with SNPs' % \
			len(markersWithSNPs))

		(columns, rows) = self.results[resultIndex]
		keyCol = Gatherer.columnNumber (columns, MARKER_KEY)
		termCol = Gatherer.columnNumber (columns, 'term')
		countCol = Gatherer.columnNumber (columns, 'myCount')

		byMarker = {}			# byMarker[marker key] = {}

		all = 'All PCR and RFLP'	# keys per marker in byMarker
		pcr = 'PCR'
		rflp = 'RFLP'

		if MarkerSnpAssociations.isInSync():
			snp = 'SNPs within 2kb'
		else:
			snp = 'SNPs'		# not counted in 'all'

		MarkerSnpAssociations.unload()
		gc.collect()

		# first populate SNP counts for markers with SNPs (and include
		# the various locations for SNPs with multiple locations)

		for key in markersWithSNPs.keys():
			snpCount, multiSnpCount = markersWithSNPs[key]
			byMarker[key] = { snp : snpCount + multiSnpCount }

		# collate counts per marker

		for row in rows:
			term = row[termCol]
			ct = row[countCol]
			key = row[keyCol]

			if not byMarker.has_key (key):
				# note that it has no SNPs (they would have
				# already been populated otherwise)

				byMarker[key] = { snp : 0 }

			if term == 'primer':
				byMarker[key][pcr] = ct
			elif not byMarker[key].has_key(rflp):
				byMarker[key][rflp] = ct
			else:
				byMarker[key][rflp] = byMarker[key][rflp] + ct

		# free up memory for result set, once we've walked through it
		self.results[resultIndex] = (columns, [])
		del rows
		gc.collect()

		# if we had both PCR and RFLP then we need a count for the sum

		for key in byMarker.keys():
			if byMarker[key].has_key(pcr) \
			    and byMarker[key].has_key(rflp):
				byMarker[key][all] = byMarker[key][pcr] + \
					byMarker[key][rflp]
			
		# generate rows, one per marker/count pair

		orderedTerms = [ all, pcr, rflp, snp ]
		for key in byMarker.keys():
			for term in orderedTerms:
				if byMarker[key].has_key(term):
					newRow = [ key, 'Polymorphisms',
						term, byMarker[key][term], 
						self.seqNum() ]
					self.finalResults.append (newRow)
		self.report()
		self.writeSoFar()
		return

	def collateMarkerInteractions (self):

		counts = InteractionUtils.getMarkerInteractionCounts()

		fTerm = 'interacts with'

		for (key, count) in counts.items():
			self.finalResults.append ( [ key, 'Interaction',
				fTerm, count, 1 ] )

		logger.debug('Added %d rows for Interactions' % \
			len(counts))
		self.writeSoFar()

		del counts
		gc.collect()
		return

	def collateAlleleCounts (self):
		# collate the allele counts, including the special handling
		# for markers associated with alleles via 'mutation involves'
		# relationships

		counts, countSets = MarkerUtils.getAlleleCountsByType()

		orderedSets = countSets.items()
		orderedSets.sort()

		markerKeys = counts.keys()
		markerKeys.sort()

		setType = 'Alleles'
		i = 0

		for markerKey in markerKeys:
			i = i + len(counts[markerKey])

			for (seqNum, countType) in orderedSets:
				if counts[markerKey].has_key(countType):
					row = [ markerKey,
						setType,
						countType, 
						counts[markerKey][countType],
						seqNum ]

					self.finalResults.append (row)

		del counts
		del countSets
		del orderedSets
		del markerKeys
		gc.collect()

		logger.debug('Added %d rows for Alleles' % i) 
		self.writeSoFar() 
		return

	def writeSoFar (self):
		if self.finalResults:
			self.outFile.writeToFile (fieldOrder,
				self.finalColumns, self.finalResults)

			logger.debug('Wrote %d rows to output file' % \
				len(self.finalResults))

			self.finalResults = []
			gc.collect()
		else:
			logger.debug('Nothing to write')
		return

	def go (self):
		# override the go() method, so we can customize how we deal
		# with the output data file

		self.preprocessCommands()
		logger.info('Pre-processed queries')

		self.results = Gatherer.executeQueries (self.cmds)
		logger.info('Finished queries of source %s db' % \
			Gatherer.SOURCE_DB)

		self.outFile = OutputFile.OutputFile('marker_count_sets')
		logger.info('Created output file: %s' % self.outFile.getPath())

		self.collateResults()
		self.postprocessResults()
		self.writeSoFar()
		self.outFile.close()

		print '%s %s' % (self.outFile.getPath(), 'marker_count_sets') 
		return

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

		self.i = 0		# counter for ordering of rows
		self.j = 0		# counter of result sets

		# first do sets which are special cases:  

		self.collateResultsByTheilerStage(0)
		self.collateAssaysByAssayType(1)
		self.collateResultsByAssayType(2)

		ADMapper.unload()		# free unneeded memory

		self.collateReagents(3)
		self.collatePolymorphisms(4)
		self.collateMarkerInteractions()
		self.collateAlleleCounts()

		# the remaining sets (5 to the end) have a standard format
		# and can be done in a nested loop

# currently no queries from 5...
#		for (columns, rows) in self.results[5:]:
#			keyCol = Gatherer.columnNumber (columns, MARKER_KEY)
#			setCol = Gatherer.columnNumber (columns, SET_TYPE)
#			typeCol = Gatherer.columnNumber (columns, COUNT_TYPE)
#			countCol = Gatherer.columnNumber (columns, COUNT)
#
#			for row in rows:
#				newRow = [ row[keyCol], row[setCol],
#					row[typeCol], row[countCol], 
#					self.seqNum() ]
#				self.finalResults.append (newRow)
#			self.report()
		return

###--- globals ---###

cmds = [
	# 0. expression results by theiler stages 
	'''select ge._Marker_key,
		gs._Structure_key,
		count(distinct ge._Expression_key) as %s
	from gxd_expression ge,
		gxd_structure gs,
		gxd_theilerstage ts
	where ge.isForGXD = 1
		and ge._Structure_key = gs._Structure_key
		and gs._Stage_key = ts._Stage_key
		and exists (select 1 from mrk_marker m
			where m._Marker_key = ge._Marker_key)
	group by ge._Marker_key, gs._Structure_key
	order by ge._Marker_key, gs._Structure_key''' % COUNT,

	# 1. expression assays by type
	'''select ge._Marker_key,
		ge._Structure_key,
		gat.assayType as %s,
		count(distinct ge._Assay_key) as %s
	from gxd_expression ge,
		gxd_assaytype gat
	where ge._AssayType_key = gat._AssayType_key
		and ge.isForGXD = 1
		and exists (select 1 from mrk_marker m
			where m._Marker_key = ge._Marker_key)
	group by ge._Marker_key, gat.assayType, ge._Structure_key
	order by ge._Marker_key''' % (COUNT_TYPE, COUNT),

	# 2. expression results by type
	'''select ge._Marker_key,
		ge._Structure_key,
		gat.assayType as %s,
		count(distinct ge._Expression_key) as %s
	from gxd_expression ge,
		gxd_assaytype gat
	where ge._AssayType_key = gat._AssayType_key
		and ge.isForGXD = 1
		and exists (select 1 from mrk_marker m
			where m._Marker_key = ge._Marker_key)
	group by ge._Marker_key, gat.assayType, ge._Structure_key
	order by ge._Marker_key''' % (COUNT_TYPE, COUNT),

	# 3. counts of reagents by type (these need to be grouped in code, but
	# this will give us the raw counts)
	'''select pm._Marker_key,
		vt.term,
		count(distinct pp._Probe_key) as myCount
	from prb_marker pm,
		prb_probe pp,
		voc_term vt
	where pp._SegmentType_key = vt._Term_key
		and pm._Probe_key = pp._Probe_key
		and exists (select 1 from mrk_marker m
			where m._Marker_key = pm._Marker_key)
	group by pm._Marker_key, vt.term''',

	# 4. counts of RFLP/PCR polymorphisms by type
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
		and exists (select 1 from mrk_marker m
			where m._Marker_key = rflv._Marker_key)
	group by rflv._Marker_key, t.term''',
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
