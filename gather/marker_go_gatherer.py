#!/usr/local/bin/python
# 
# gathers data for most of the 'marker_go_*' tables in the front-end database

import Gatherer
import VocabSorter
import logger

###--- Functions ---###

goAnnotKeys = {}

def getGoAnnotationKey (annotKey, evidenceTermKey, inferredFrom):
	global goAnnotKeys

	tpl = (annotKey, evidenceTermKey, inferredFrom)
	if not goAnnotKeys.has_key(tpl):
		goAnnotKeys[tpl] = len(goAnnotKeys) + 1
	return goAnnotKeys[tpl]

###--- Classes ---###

class MarkerGoGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for most of the marker_go_* tables
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for markers' GO
	#	annotations, collates results, writes tab-delimited text files

	def findAnnotations (self):
		# find the basic annotation data for GO/marker associations
		# Returns: 3-item tuple:
		#	(list of marker keys,
		#	dictionary mapping marker keys to list of annot keys,
		#	dictionary mapping annot keys to [ marker key,
		#		DAG name, qualifier, term, term ID ]
		#	)

		cols, rows = self.results[0]

		mkeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		termCol = Gatherer.columnNumber (cols, 'term')
		idCol = Gatherer.columnNumber (cols, 'termID')
		akeyCol = Gatherer.columnNumber (cols, '_Annot_key')
		qkeyCol = Gatherer.columnNumber (cols, '_Qualifier_key')
		dagCol = Gatherer.columnNumber (cols, 'dagName')

		markers = {}		# marker key -> [ annotation keys ]
		annotations = {}	# annot key -> [ marker key, dag,
					# ... qualifier, term, term ID ]

		for row in rows:
			markerKey = row[mkeyCol]
			annotationKey = row[akeyCol]

			if markers.has_key(markerKey):
				markers[markerKey].append (annotationKey)
			else:
				markers[markerKey] = [ annotationKey ]

			annotations[annotationKey] = [
				markerKey,
				row[dagCol],
				Gatherer.resolve (row[qkeyCol]),
				row[termCol],
				row[idCol],
				]

		logger.debug ('Found %d annotations for %d markers' % (
			len(annotations), len(markers)) )

		markerKeys = markers.keys()
		markerKeys.sort()

		logger.debug ('Sorted %d marker keys' % len(markerKeys))

		return markerKeys, markers, annotations

	def findReferences (self):
		# collate the reference and evidence term info for each
		# annotation
		# Returns: 4-item tuple:
		#	(dict mapping annotation key -> [ GO annot keys ],
		#	dict mapping GO annot key -> [ (refs key, jnum), ...],
		#	dict mapping GO annot key -> (evidence key, evidence
		#		abbrev, evidence term),
		#	dict mapping evidence abbrev -> evidence term
		#	)

		cols, rows = self.results[1]

		akeyCol = Gatherer.columnNumber (cols, '_Annot_key')
		termCol = Gatherer.columnNumber (cols, 'term')
		abbrevCol = Gatherer.columnNumber (cols, 'abbreviation')
		evidenceCol = Gatherer.columnNumber(cols, '_EvidenceTerm_key')
		inferredCol = Gatherer.columnNumber (cols, 'inferredFrom')
		refCol = Gatherer.columnNumber (cols, '_Refs_key')
		jnumCol = Gatherer.columnNumber (cols, 'jnumID')
		
		annotToGoAnnot = {}	# annot key -> [ GO annot keys ]
		goAnnotToRefs = {}	# GO annot key -> [ (refs key, jnum) ]
		goAnnotToEvidence = {}	# GO annot key -> (evidence key,
					# ... evidence abbrev, evidence term)
		evidenceCodes = {}	# abbreviation -> full term

		for row in rows:
			annotKey = row[akeyCol]
			code = row[abbrevCol]
			evidence = row[evidenceCol]

			# get the special key we are generating for each
			# unique (annotation key, evidence term key,
			# inferred from) tuple

			goAnnotKey = getGoAnnotationKey (annotKey,
				evidence, row[inferredCol])

			# map each annotation to its special GO annotation
			# keys that we are generating

			if annotToGoAnnot.has_key (annotKey):
			    if goAnnotKey not in annotToGoAnnot[annotKey]:
				annotToGoAnnot[annotKey].append (goAnnotKey)
			else:
				annotToGoAnnot[annotKey] = [ goAnnotKey ]

			# track all the abbreviations and terms we are
			# encountering for evidence

			if not evidenceCodes.has_key (code):
				evidenceCodes[code] = row[termCol]

			# keep references for each special GO annotation key

			if not goAnnotToRefs.has_key (goAnnotKey):
				goAnnotToRefs[goAnnotKey] = [ (row[refCol],
					row[jnumCol]) ]
			else:
				goAnnotToRefs[goAnnotKey].append (
					(row[refCol], row[jnumCol]) )

			# keep evidence for each special GO annotation key

			if not goAnnotToEvidence.has_key(goAnnotKey):
				goAnnotToEvidence[goAnnotKey] = (
					evidence, code, row[termCol] )

		logger.debug ('Found %d annotations for %d rows' % (
			len(annotToGoAnnot), len(goAnnotToRefs)) )

		return annotToGoAnnot, goAnnotToRefs, goAnnotToEvidence, \
			evidenceCodes

	def findSpecialIDs (self):
		# get the set of "inferred from" IDs which are sequence IDs
		# from certain providers that we want to redirect to our own
		# sequence detail page

		cols, rows = self.results[3]

		idCol = Gatherer.columnNumber (cols, 'accID')
		ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')

		specialIDs = {}
		for row in rows:
			specialIDs[row[idCol]] = row[ldbKeyCol]

		logger.debug ('Found %d seq IDs to send to MGI' % \
			len(specialIDs))
		return specialIDs

	def findInferredFromIDs (self):
		# get the various inferred-from IDs for each annotation

		seqIDs = self.findSpecialIDs()

		cols, rows = self.results[2]

		akeyCol = Gatherer.columnNumber (cols, '_Annot_key')
		evidenceCol = Gatherer.columnNumber(cols, '_EvidenceTerm_key')
		idCol = Gatherer.columnNumber (cols, 'accID')
		ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
		ldbCol = Gatherer.columnNumber (cols, 'logical_db')
		inferredCol = Gatherer.columnNumber (cols, 'inferredFrom')
		preferredCol = Gatherer.columnNumber (cols, 'preferred')

		ids = {}	# GO annot key -> list of (acc ID,
				# ... logical db, preferred)

		for row in rows:
			annotKey = row[akeyCol]
			evidence = row[evidenceCol]
			id = row[idCol]
			ldbKey = row[ldbKeyCol]
			ldbName = row[ldbCol]

			# get the special key we are generating for each
			# unique (annotation key, evidence term key,
			# inferred from) tuple

			goAnnotKey = getGoAnnotationKey (annotKey,
				evidence, row[inferredCol])

			# for certain sequences, we want to have any links go
			# to MGI rather than to the logical database

			if seqIDs.has_key(id):
				if seqIDs[id] == ldbKey:
					ldbKey = 1
					ldbName = 'MGI'

			tpl = (id, ldbName, row[preferredCol])

			# store the ID info for this GO annot key

			if not ids.has_key(goAnnotKey):
				ids[goAnnotKey] = [ tpl ]
			else:
				ids[goAnnotKey].append(tpl)

		logger.debug ('Found inferred-from IDs for %d records' % \
			len(ids))
		return ids

	def collateResults (self):
		# process query 0
		markerKeys, markerToAnnotations, annotations = \
			self.findAnnotations()

		# process query 1
		annotToGoAnnot, goAnnotToRefs, goAnnotToEvidence, \
			evidenceCodes = self.findReferences()

		# process query 2
		ids = self.findInferredFromIDs()

		# define the columns and rows for the four tables...

		# table 1 : marker_go_annotation
		mga_cols = [ 'go_annotation_key', 'marker_key', 'dag_name',
			'qualifier', 'term', 'term_id', 'evidence_code',
			'reference_count', 'inferred_id_count', 'sequence_num'
			]
		mga = []

		# table 2 : marker_go_evidence_terms
		mget_cols = [ 'unique_key', 'marker_key', 'abbreviation',
			'term', 'sequence_num' ]
		mget = []

		# table 3 : marker_go_inferred_from_id
		mgifi_cols = [ 'unique_key', 'go_annotation_key',
			'logical_db', 'acc_id', 'preferred', 'private',
			'sequence_num' ]
		mgifi = []

		# table 4 : marker_go_reference
		mgr_cols = [ 'unique_key', 'go_annotation_key',
			'reference_key', 'jnum_id', 'sequence_num' ]
		mgr = []

		# now we need to populate the rows of those tables based on
		# the data we found
		
		for markerKey in markerKeys:
			# evidence term abbreviations
			markerAbbrev = {}

			# for each marker, get all basic annotations
			annotationKeys = markerToAnnotations[markerKey]

			# walk through basic annotations for the marker
			for annotationKey in annotationKeys:
				[ mkey, dagName, qualifier, term, termID ] = \
					annotations[annotationKey]

				# look up the generated keys for the details
				# of the annotations
				goAnnotKeys = annotToGoAnnot[annotationKey]

				for goAnnotKey in goAnnotKeys:
					# list of references
					refs = goAnnotToRefs[goAnnotKey]

					# evidence term data
					(evidKey, evidAbbrev, evidTerm) = \
						goAnnotToEvidence[goAnnotKey]

					if ids.has_key(goAnnotKey):
						myIds = ids[goAnnotKey]
					else:
						myIds = []

					markerAbbrev[evidAbbrev] = 1

					# add basic row
					mga_row = [ goAnnotKey, markerKey,
						dagName, qualifier, term,
						termID, evidAbbrev,
						len(refs), len(myIds),
						len(mga) + 1 ]
					mga.append (mga_row)

					# add ID rows
					for (id, ldbName, preferred) in myIds:
						mgifi_row = [
							len(mgifi) + 1,
							goAnnotKey, ldbName,
							id, preferred, 0,
							len(mgifi) + 1
							]
						mgifi.append (mgifi_row)

					# add reference rows
					for (refKey, jnum) in refs:
						mgr_row = [
							len(mgr) + 1,
							goAnnotKey, refKey,
							jnum, len(mgr) + 1 ]
						mgr.append (mgr_row)

			# build set of evidence codes used for this marker

			abbrevs = markerAbbrev.keys()
			abbrevs.sort()

			for abbrev in abbrevs:
				mget_row = [
					len(mget) + 1,
					markerKey, abbrev,
					evidenceCodes[abbrev],
					len(mget) + 1 ]
				mget.append (mget_row)

		# add the files into the list for output
		self.output.append ( (mga_cols, mga) )
		self.output.append ( (mgifi_cols, mgifi) )
		self.output.append ( (mgr_cols, mgr) )
		self.output.append ( (mget_cols, mget) )

		logger.debug ('Prepared output files')
		return

###--- globals ---###

cmds = [
	# 0. basic GO term to marker annotation data
	'''select distinct va._Object_key as _Marker_key,
		vt.term,
		aa.accID as termID,
		va._Annot_key,
		va._Qualifier_key,
		dd.name as dagName
	from voc_annot va,
		voc_term vt,
		acc_accession aa,
		dag_node dn,
		dag_dag dd
	where va._Term_key = vt._Term_key
		and va._Term_key = aa._Object_key
		and va._Term_key = dn._Object_key
		and dn._DAG_key = dd._DAG_key
		and va._AnnotType_key = 1000
		and aa._MGIType_key = 13
		and aa.preferred = 1''',

	# 1. reference(s) and evidence term(s) for each annotation
	'''select va._Annot_key,
		ve._AnnotEvidence_key,
		bc.jnumID,
		bc._Refs_key,
		bc.numericPart,
		vt.abbreviation,
		vt.term,
		ve.inferredFrom,
		ve._EvidenceTerm_key
	from voc_annot va,
		voc_evidence ve,
		bib_citation_cache bc,
		voc_term vt
	where va._AnnotType_key = 1000
		and va._Annot_key = ve._Annot_key
		and ve._Refs_key = bc._Refs_key
		and ve._EvidenceTerm_key = vt._Term_key
	order by ve._AnnotEvidence_key, bc.numericPart''',

	# 2. 'inferred from' IDs for each annotation/evidence pair
	'''select va._Annot_key,
		ve._AnnotEvidence_key,
		aa.accID,
		aa._LogicalDB_key,
		aa.prefixPart,
		aa.numericPart,
		aa.preferred,
		ldb.name as logical_db,
		ve.inferredFrom,
		ve._EvidenceTerm_key
	from voc_annot va,
		voc_evidence ve,
		acc_accession aa,
		acc_logicaldb ldb
	where va._Annot_key = ve._Annot_key
		and va._AnnotType_key = 1000
		and ve._AnnotEvidence_key = aa._Object_key
		and aa._MGIType_key = 25
		and aa._LogicalDB_key = ldb._LogicalDB_key
	order by ve._AnnotEvidence_key, aa.prefixPart, aa.numericPart''',

	# 3. get the set of IDs which are sequence IDs from NCBI, EMBL, and
	# UniProt; for these, we will need to replace the logical database
	# with the MGI logical database to link to our sequence detail page
	'''select distinct aa.accID,
		aa._LogicalDB_key
	from acc_accession aa,
		acc_accession aa2
	where aa._MGIType_key = 25
		and aa.accID = aa2.accID
		and aa2._LogicalDB_key in (68, 9, 13)
		and aa2._MGIType_key = 19''',
	]

# definition of output files, each as:
#	(filename prefix, list of fieldnames, table name)
files = [
	('marker_go_annotation',
		[ 'go_annotation_key', 'marker_key', 'dag_name',
			'qualifier', 'term', 'term_id', 'evidence_code',
			'reference_count', 'inferred_id_count',
			'sequence_num' ],
		'marker_go_annotation'),

	('marker_go_inferred_from_id',
		[ 'unique_key', 'go_annotation_key',
			'logical_db', 'acc_id', 'preferred', 'private',
			'sequence_num' ],
		'marker_go_inferred_from_id'),

	('marker_go_reference',
		[ 'unique_key', 'go_annotation_key', 'reference_key',
			'jnum_id', 'sequence_num' ],
		'marker_go_reference'),

	('marker_go_evidence_terms',
		[ 'unique_key', 'marker_key', 'abbreviation', 'term',
			'sequence_num' ],
		'marker_go_evidence_terms')
	]

# global instance of a MarkerGoGatherer
gatherer = MarkerGoGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
