#!/usr/local/bin/python
# 
# gathers data for the phenotype-releated tables in the front-end database:
# allele_grid_row, allele_grid_column, allele_grid_value,
# genotype_phenotype, genotype_phenotype_note, genotype_phenotype_reference

import Gatherer
import logger
import GenotypeClassifier
import VocabSorter

###--- Global Constants ---###

CHECK = 'x'		# check mark in pheno grid
NOT = 'not'		# not symbol in pheno grid
NORMAL = 'N'		# normal symbol in pheno grid

###--- Functions ---###

def descendsFrom (descendentTerm, ancestorTerm):
	return VocabSorter.isDescendentOf (descendentTerm, ancestorTerm)

GRID_ROW_KEYS = {}
def getGridRowKey (alleleKey, termKey, term, header = None):
	global GRID_ROW_KEYS

	key = (alleleKey, termKey, term, header)

	if not GRID_ROW_KEYS.has_key (key):
		GRID_ROW_KEYS[key] = len(GRID_ROW_KEYS) + 1

	return GRID_ROW_KEYS[key]

GRID_COLUMN_KEYS = {}
def getGridColumnKey (alleleKey, genotypeKey):
	global GRID_COLUMN_KEYS

	key = (alleleKey, genotypeKey)

	if not GRID_COLUMN_KEYS.has_key (key):
		GRID_COLUMN_KEYS[key] = len(GRID_COLUMN_KEYS) + 1

	return GRID_COLUMN_KEYS[key]

def extractRowsIterative (sortableRows, baseList):
	# recursive version was too slow (17 minutes), so rewriting as an
	# iterative function...  (this was slower -- 20 minutes)

	sortableRows.sort()
	output = []

	# use stack-based processing; add and pop from head of stack

	stack = sortableRows[:]		# copy into stack
	while stack:
		[ sortFields, row, subrows ] = stack[0]
		if subrows:
			subrows.sort()
			stack = subrows + stack[1:]
		else:
			stack = stack[1:]

		output.append (baseList[row])

	return output

def extractRowsRecursive (sortableRows, baseList):
	sortableRows.sort()

	output = []

	for [sortFields, row, subrows] in sortableRows:
		output.append (baseList[row])
		if subrows:
			output = output + extractRows(subrows, baseList)

	return output

def extractRows (sortableRows, baseList):
#	return extractRowsIterative (sortableRows, baseList)
	return extractRowsRecursive (sortableRows, baseList)

def numberRows (rows):
	i = 0
	for row in rows:
		i = i + 1
		row.append (i)
	return rows

def sortGenotypePhenotypeRows (gpRows):
	# At this point, 'gpRows' is sorted by genotype, by header term, and
	# by depth-first search under the header terms.  This was useful for
	# grouping terms under the headers in a tree structure, but can leave
	# apparent sorting issues, as our indentation level is key.  Terms at
	# the same indentation level should be sorted alphabetically (along
	# with the rows indented under them), even if those terms are actually
	# at different levels of the DAG.

	logger.debug ('Bypassing extra sort of gpRows')
	if True:
		return numberRows(gpRows)

#	logger.debug ('Received %d gpRows to sort' % len(gpRows))
#
#	# Each row in gpRows has:
#	#   genotypePhenotypeKey, genotypeKey, isHeaderTerm, term, termID,
#	#   indentationLevel, referenceCount, noteCount
#	# We will compute and append: sequenceNum
#
#	sortKeys = []
#	for row in gpRows:
#		# sort by genotype key, indent level, lowercase term
#		sortKeys.append ( (row[1], row[5], row[3].lower() ) )
#
#	logger.debug ('Collected %d sortKeys' % len(sortKeys))
#
#	sortKeys.sort()
#	logger.debug ('Sorted %d sortKeys' % len(sortKeys))
#
#	sortKeyDict = {}
#	i = 0
#	for key in sortKeys:
#		i = i + 1
#		sortKeyDict[key] = i
#	logger.debug ('Put %d sortKeys into dict' % len(sortKeyDict))
#
#	# collate our rows into a standard, sortable structure, each as:
#	#	[ [sortable fields], original row, [ sub rows ] ]
#	
#	headers = []	# outer list of sortable row structures
#	rowStack = []	# stack of rows we're working with now (one per
#			# indentation level)
#
#	i = 0		# position of 'row' in 'gpRows'
#
#	for row in gpRows:
#		genotypeKey = row[1]
#		indent = row[5]
#		lowerTerm = row[3].lower()
#
#		sortVals = sortKeyDict[(genotypeKey, indent, lowerTerm)]
#		sortableRow = [ sortVals, i, [] ]
#		currentIndent = max(0,len(rowStack) - 1)
#
#		# if a new header row, add it to the header list (all others
#		# will appear as subrows for a header row)
#
#		if indent == 0:
#			headers.append (sortableRow)
#
#		# if this row is at the same or a lesser indentation level, we
#		# need to pop off items until we get back to a parent node
#
#		if indent <= currentIndent:
#			rowStack = rowStack[:indent]
#
#		# otherwise, we're at a greater indent level, so we can leave
#		# the stack alone
#
#		# if we have a rowStack, then the last row is the parent to
#		# the current row
#
#		if rowStack:
#			parentRow = rowStack[-1]
#			parentRow[-1].append (sortableRow)
#
#		# now this row will be the potential parent for the next row
#
#		rowStack.append (sortableRow)
#
#		i = i + 1
#		
#	logger.debug ('Collected rows under %d header rows' % len(headers))
#
#	# now build our output rows
#
#	output = extractRows (headers, gpRows)
#	logger.debug ('Expanded back to %d rows' % len(output))
#
#	output = numberRows (output)
#	logger.debug ('Numbered %d rows' % len(output))
#
#	logger.debug ('Returning %d rows for gpRows' % len(output))
#	return output

def sortAlleleGenotypeRows (agrRows):
	# At this point, 'agrRows' is sorted by allele key, by header term,
	# and by depth-first search under the header terms.  This was useful
	# for grouping terms under the headers in a tree structure, but can
	# leave apparent sorting issues, as our indentation level is key.
	# Terms at the same indentation level should be sorted alphabetically
	# (along with the rows indented under them), even if those terms are
	# actually at different levels of the DAG.

	logger.debug ('Bypassing extra sort for agrRows')
	if True:
		return numberRows(agrRows)

#	logger.debug ('Received %d agrRows to sort' % len(agrRows))
#
#	# Each row in agrRows has:
#	#   gridRowKey, alleleKey, term, termID, isHeader, indentationLevel
#	# We will compute and append: sequenceNum
#
#	sortKeys = []
#	for row in agrRows:
#		# sort by allele key, indent level, lowercase term
#		sortKeys.append ( (row[1], row[5], row[2].lower() ) )
#
#	logger.debug ('Collected %d sortKeys' % len(sortKeys))
#
#	sortKeys.sort()
#	logger.debug ('Sorted %d sortKeys' % len(sortKeys))
#
#	sortKeyDict = {}
#	i = 0
#	for key in sortKeys:
#		i = i + 1
#		sortKeyDict[key] = i
#	logger.debug ('Put %d sortKeys into dict' % len(sortKeyDict))
#
#	# collate our rows into a standard, sortable structure, each as:
#	#	[ [sortable fields], original row, [ sub rows ] ]
#	
#	headers = []	# outer list of sortable row structures
#	rowStack = []	# stack of rows we're working with now (one per
#			# indentation level)
#
#	i = 0		# position of 'row' in 'agrRows'
#
#	for row in agrRows:
#		alleleKey = row[1]
#		indent = row[5]
#		lowerTerm = row[2].lower()
#
#		sortVals = sortKeyDict[(alleleKey, indent, lowerTerm)]
#		sortableRow = [ sortVals, i, [] ]
#		currentIndent = max(0,len(rowStack) - 1)
#
#		# if a new header row, add it to the header list (all others
#		# will appear as subrows for a header row)
#
#		if indent == 0:
#			headers.append (sortableRow)
#
#		# if this row is at the same or a lesser indentation level, we
#		# need to pop off items until we get back to a parent node
#
#		if indent <= currentIndent:
#			rowStack = rowStack[:indent]
#
#		# otherwise, we're at a greater indent level, so we can leave
#		# the stack alone
#
#		# if we have a rowStack, then the last row is the parent to
#		# the current row
#
#		if rowStack:
#			parentRow = rowStack[-1]
#			parentRow[-1].append (sortableRow)
#
#		# now this row will be the potential parent for the next row
#
#		rowStack.append (sortableRow)
#
#		i = i + 1
#		
#	logger.debug ('Collected rows under %d header rows' % len(headers))
#
#	# now build our output rows
#
#	output = extractRows (headers, agrRows)
#	logger.debug ('Expanded back to %d rows' % len(output))
#
#	output = numberRows (output)
#	logger.debug ('Numbered %d rows' % len(output))
#
#	logger.debug ('Returning %d rows for agrRows' % len(output))
#	return output

###--- Classes ---###

class PhenotypeGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the phenotype-related data
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for phenotypes,
	#	collates results, writes tab-delimited text file

	def cacheRefInfo (self):
		# cache the J: number and its numeric part for each reference
		# cited as annotation evidence

		self.jnumID = {}	# refs key -> J: number
		self.jnumNumeric = {}	# refs key -> int from J: number

		(cols, rows) = self.results[0]

		keyCol = Gatherer.columnNumber (cols, '_Refs_key')
		numCol = Gatherer.columnNumber (cols, 'numericPart')
		idCol = Gatherer.columnNumber (cols, 'accID')

		for row in rows:
			key = row[keyCol]
			self.jnumID[key] = row[idCol]
			self.jnumNumeric[key] = row[numCol]

		logger.debug ('Cached %d J: numbers' % len(self.jnumID))
		return

	def cacheGenotypesPerAllele (self):
		# cache the set of genotype keys for each allele key

		self.genotypes = {}	# allele key -> [ genotype keys ]

		(cols, rows) = self.results[1]

		alleleCol = Gatherer.columnNumber (cols, '_Allele_key')
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')

		for row in rows:
			allele = row[alleleCol]
			genotype = row[genotypeCol]

			if not self.genotypes.has_key(allele):
				self.genotypes[allele] = [ genotype ]
			else:
				self.genotypes[allele].append (genotype)

		logger.debug ('Cached genotypes for %d alleles' % \
			len(self.genotypes))
		return

	def cacheEvidence (self):
		# cache the references and the notes for each annotation

		# annot key -> { note key : [ refs key, note type, note] }
		self.notes = {}

		# annot key -> [ refs keys ]
		self.refs = {}

		(cols, rows) = self.results[2]

		annotCol = Gatherer.columnNumber (cols, '_Annot_key')
		noteTypeCol = Gatherer.columnNumber (cols, '_NoteType_key')
		noteKeyCol = Gatherer.columnNumber (cols, '_Note_key')
		seqNumCol = Gatherer.columnNumber (cols, 'sequenceNum')
		refsKeyCol = Gatherer.columnNumber (cols, '_Refs_key')
		noteCol = Gatherer.columnNumber (cols, 'note')
		evidenceCol = Gatherer.columnNumber (cols,
			'_AnnotEvidence_key')

		for row in rows:
			refsKey = row[refsKeyCol]
			annotKey = row[annotCol]
			noteKey = row[noteKeyCol]
			note = row[noteCol]
			noteType = Gatherer.resolve (row[noteTypeCol],
				'mgi_notetype', '_NoteType_key', 'noteType')

			# collect the references for each annotation

			if self.refs.has_key(annotKey):
				if refsKey not in self.refs[annotKey]:
					self.refs[annotKey].append (refsKey)
			else:
				self.refs[annotKey] = [ refsKey ]

			# collect the note data

			if not self.notes.has_key(annotKey):
				self.notes[annotKey] = { noteKey :
					[ refsKey, noteType, note ] }
			elif not self.notes[annotKey].has_key(noteKey):
				self.notes[annotKey][noteKey] = [ refsKey,
					noteType, note ]
			else:
				[ refsKey, noteType, oldNote] = \
					self.notes[annotKey][noteKey]
				self.notes[annotKey][noteKey] = [
					refsKey, noteType, oldNote + note ]

		logger.debug ('Collected refs + notes for %d annotations' % \
			len(self.notes) )
		return

	def cacheAnnotations (self):
		# process the main annotation query to cache our annotations

		cols, rows = self.results[3]

		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		seqNumCol = Gatherer.columnNumber (cols, 'sequenceNum')
		headerTermCol = Gatherer.columnNumber (cols, 'headerTerm')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		termCol = Gatherer.columnNumber (cols, 'term')
		annotKeyCol = Gatherer.columnNumber (cols, '_Annot_key')
		qualifierKeyCol = Gatherer.columnNumber (cols,
			'_Qualifier_key')

		# we need:
		# 1. annotations for each genotype, sorted by header term
		#	as specified by curators, then by depth-first ordering

		# genotype key -> [ (header seq num, term DFS seq num,
		#	annot key, header term, term key, term,
		#	qualifier key), ... ]
		self.genotypeData = {}

		# because our query is not distinct, we need to keep track of
		# which header key/annotation key/genotype key triples, and
		# skip them if we encounter them again
		alreadyDone = {}	# (annot key, header, genotype) -> 1

		for row in rows:
			genotypeKey = row[genotypeKeyCol]

			key = (row[annotKeyCol], row[headerTermCol],
				genotypeKey)

			if alreadyDone.has_key(key):
				continue

			record = [ row[seqNumCol],
				VocabSorter.getSequenceNum(row[termKeyCol]),
				row[annotKeyCol],
				row[headerTermCol], 
				row[termKeyCol],
				row[termCol],
				row[qualifierKeyCol] ]

			if self.genotypeData.has_key (genotypeKey):
				self.genotypeData[genotypeKey].append (record)
			else:
				self.genotypeData[genotypeKey] = [ record ]

			alreadyDone[key] = 1

		logger.debug ('Got annotations for %d genotypes' % \
			len(self.genotypeData) )

		# sort the annotations for each genotype

		genotypes = self.genotypeData.keys()
		for genotypeKey in genotypes:
			self.genotypeData[genotypeKey].sort()

		logger.debug ('Sorted annotations for genotypes')
		return

	def addAnnotationIndents (self):
		# adds indentation level to each annotation in
		# self.genotypeData; leftmost is 1, with 1 extra indent for
		# each descendent

		genotypes = self.genotypeData.keys()
		genotypes.sort()

		mx = 0

		# relies on the annotations for each genotype being sorted by
		# header term then DFS by term

		for genotype in genotypes:
		    annotations = self.genotypeData[genotype]
#		    logger.debug (str(annotations))
		    parents = []

		    for annotation in annotations:
			termKey = annotation[4]
			indent = None

			while parents and (not indent):
			    if descendsFrom(termKey, parents[-1]):
				indent = len(parents) + 1
				parents.append (termKey)
			    else:
				parents = parents[:-1]

			if not indent:
			    indent = 1
			    parents.append (termKey)

			mx = max(indent, mx)
			annotation.append (indent)

		logger.debug ('Computed annotation indents (max %d)' % mx)
		return

	def sortGenotypesPerAllele (self):
		# alters self.genotypes to be ordered appropriate for each
		# allele; these will then be ready to be the columns for the
		# phenotype grid and the ordered set of phenotypes for the
		# genotype details section of the allele detail page

		alleleKeys = self.genotypes.keys()

		for allele in alleleKeys:
			myGenotypes = []
			for genotype in self.genotypes[allele]:
				myGenotypes.append ( (
					GenotypeClassifier.getSequenceNum (
						allele, genotype),
					genotype) )

			myGenotypes.sort()

			self.genotypes[allele] = map(lambda y : y[1],
				myGenotypes)

		logger.debug ('Sorted genotypes for %d alleles' % \
			len(self.genotypes))
		return

	def cacheTermsPerGenotype (self):
		# cache the terms annotated to each genotype, and their
		# header terms

		# genotype key -> { term key -> 1 }
		self.terms = {}

		# genotype key -> { header term -> 1 }
		self.headers = {}

		genotypeKeys = self.genotypeData.keys()
		genotypeKeys.sort()
		
		for genotype in genotypeKeys:
			annotations = self.genotypeData[genotype]

			self.terms[genotype] = {}
			self.headers[genotype] = {}

			for annotation in annotations:
				header = annotation[3]
				termKey = annotation[4]

				self.terms[genotype][termKey] = 1
				self.headers[genotype][header] = 1

		logger.debug ('Cached terms for %d genotypes' % \
			len(self.terms) )
		return

	def cacheTermsPerAllele (self):
		# generate the list of row labels for the phenotype grid for
		# each allele (the list of unique terms per allele)

		mx = 0

		# allele key -> ordered list of [term key, term, indent level]
		# triples, with headers mixed in as appropriate.  Headers will
		# have a None term key and a 0 indent level.
		self.alleleTerms = {}

		# for the purposes of the pheno grid, the headers should be
		# in alphabetic order (except that 'normal phenotype' should
		# go to the bottom)

		alleleKeys = self.genotypes.keys()
		for allele in alleleKeys:
			genotypeKeys = self.genotypes[allele]

			headers = {}
			alleleData = []
			parents = []

			for genotype in genotypeKeys:
			    if not self.genotypeData.has_key(genotype):
				    continue
			    annotations = self.genotypeData[genotype]
			    for annotation in annotations:

				header = annotation[3]
				termKey = annotation[4]
				term = annotation[5]
				termSeqNum = \
					VocabSorter.getSequenceNum(termKey)

				if headers.has_key(header):
					headers[header][termSeqNum] = (
						termKey, term)
				else:
					headers[header] = { termSeqNum :
						(termKey, term) }

			# headers now has a dictionary with sortable keys
			# to put the terms in DFS ordering

			headerTerms = headers.keys()
			headerTerms.sort()

			# move 'normal phenotype' to the end of the list
			if 'normal phenotype' in headerTerms:
				headerTerms.remove('normal phenotype')
				headerTerms.append('normal phenotype')

			for header in headerTerms:
			    terms = headers[header].items()
			    terms.sort()

			    alleleData.append ( [ None, header, 0 ] )
			    parents = []

			    for (sortVal, (termKey, term)) in terms:
			        indent = None	# indent level for this term
				while parents and (not indent):
				    if descendsFrom(termKey, parents[-1]):
					indent = len(parents) + 1
					parents.append (termKey)
				    else:
					parents = parents[:-1]

				if not indent:
			   	    indent = 1

				    # if this is an annotation to a header
				    # term, then we do not want anything to be
				    # indented under it (so do not add it to
				    # the parents list).

				    if header != term:
					parents.append (termKey)

				mx = max(mx, indent)
				alleleData.append ([ termKey, term, indent ])

			self.alleleTerms[allele] = alleleData

		logger.debug ('Collated terms for %d alleles (max %d)' % (
			len(self.alleleTerms), mx) )
		return

	def cacheIDs (self):
		# cache the IDs for the terms used in annotations

		# term key -> primary ID
		self.ids = {}

		# cache non-header terms by key

		cols, rows = self.results[4]

		keyCol = Gatherer.columnNumber (cols, '_Term_key')
		idCol = Gatherer.columnNumber (cols, 'accID')

		for row in rows:
			if not self.ids.has_key (row[keyCol]):
				self.ids[row[keyCol]] = row[idCol]

		# cache header terms by term

		cols, rows = self.results[5]

		termCol = Gatherer.columnNumber (cols, 'term')
		idCol = Gatherer.columnNumber (cols, 'accID')

		for row in rows:
			if not self.ids.has_key (row[termCol]):
				self.ids[row[termCol]] = row[idCol]

		logger.debug ('Cached %d term IDs' % len(self.ids))
		return

	def buildGenotypeTables (self):
		# build output for genotype_phenotype,
		# genotype_phenotype_note, and genotype_phenotype_reference
		# tables

		# genotype_phenotype definition
		gpCols = [ 'genotypePhenotypeKey', 'genotypeKey',
			'isHeaderTerm', 'term', 'termID', 'indentationLevel',
			'referenceCount', 'noteCount', 'sequenceNum' ]
		gpRows = []

		# genotype_phenotype_note definition
		gpnCols = [ 'genotypePhenotypeKey', 'note', 'jnumID',
			'sequenceNum' ]
		gpnRows = []

		# genotype_phenotype_reference definition
		gprCols = [ 'genotypePhenotypeKey', 'referenceKey',
			'jnumID', 'sequenceNum' ]
		gprRows = []

		# all genotype keys with annotations
		genotypeKeys = self.genotypeData.keys()

		# genotype_phenotype_key, as generated for each row in
		# the genotype_phenotype table
		gpKey = 0

		for genotype in genotypeKeys:
		    prevHeader = None	# previous header term

		    for annotation in self.genotypeData[genotype]:
			gpKey = gpKey + 1

			annotKey = annotation[2]
			header = annotation[3]
			termKey = annotation[4]
			term = annotation[5]
			qualifier = annotation[6]
			indent = annotation[7]

			if qualifier:
				qualifier = Gatherer.resolve(qualifier)

			# if this is our first encounter with a new header,
			# then add a row for it first.  also need a new one if
			# this is the same header, but a different genotype

			if header != prevHeader:
				# if our header ends with ' phenotype', then
				# we need to trim it off when we add it to the
				# table

				gpRows.append ( [ gpKey, genotype, 1,
				    header.replace (' phenotype', ''),
				    self.ids[header], 0, 0, 0 ] )

				gpKey = gpKey + 1
				prevHeader = header

			# add any notes and remember a count of them

			if self.notes.has_key(annotKey):
			    noteCount = len(self.notes[annotKey])

			    j = 0
			    for (noteKey, [refsKey, noteType, note]) in \
				self.notes[annotKey].items():

				# background sensitivity notes need a prefix

				if noteType.lower() == 'background sensitivity':
				    note = 'Background Sensitivity: ' + note

				j = j + 1
				gpnRows.append ( [ gpKey, note, 	
				    self.jnumID[refsKey], j ] )
			else:
			    noteCount = 0

			# add any references and remember a count of them

			if self.refs.has_key(annotKey):
			    refCount = len(self.refs[annotKey])

			    sortableRefs = []
			    for ref in self.refs[annotKey]:
				sortableRefs.append ( (self.jnumNumeric[ref],
				    ref) )
			    sortableRefs.sort()

			    k = 0
			    for (jnumInt, ref) in sortableRefs:
				k = k + 1
				gprRows.append ( [ gpKey, ref,
				    self.jnumID[ref], k ] )
			else:
			    refCount = 0

			# add the row to the base table

			# if our annotation is to a header term, then we need
			# to append a ' phenotype' suffix if there's not
			# already one there

			if (term == header) and \
				(not term.endswith('phenotype')):
					term = term + ' phenotype' 

			gpRows.append ( [ gpKey, genotype, 0, term,
				self.ids[termKey], indent, refCount, 
				noteCount ] )

		logger.debug ('Built rows for %d genotypes' % \
			len(genotypeKeys))

		# add our compiled data to our output

		gpRows = sortGenotypePhenotypeRows (gpRows)

		self.output.append ( [ gpCols, gpRows ] )
		self.output.append ( [ gpnCols, gpnRows ] )
		self.output.append ( [ gprCols, gprRows ] )

		return

	def buildPhenotypeTables (self):
		# build output for allele_grid_row, allele_grid_column, and
		# allele_grid_value tables

		# allele_grid_column definition
		agcCols = [ 'gridColumnKey', 'alleleKey', 'alleleGenotypeKey',
			'genotypeType', 'genotypeDesignation', 'sequenceNum' ]
		agcRows = []

		# allele_grid_row definition
		agrCols = [ 'gridRowKey', 'alleleKey', 'term', 'termID',
			'isHeader', 'indentationLevel', 'sequenceNum' ]
		agrRows = []

		# allele_grid_value definition
		agvCols = [ 'gridRowKey', 'gridColumnKey', 'alleleKey',
			'value' ]
		agvRows = []

		# fill our rows
		
		alleleKeys = self.genotypes.keys()
		alleleKeys.sort()

		for allele in alleleKeys:
		    genotypeKeys = self.genotypes[allele]

		    # build allele_grid_column rows for each allele / genotype
		    # pair:

		    for genotype in genotypeKeys:
			gridColKey = getGridColumnKey (allele, genotype)

			gType = GenotypeClassifier.getClass (genotype)
			gDesignation = GenotypeClassifier.getDesignation (
				allele, genotype)
			agKey = GenotypeClassifier.getAlleleGenotypeKey (
				allele, genotype)
			agSeqNum = GenotypeClassifier.getSequenceNum (
				allele, genotype)

			agcRows.append ( [ gridColKey, allele, agKey, gType,
				gDesignation, agSeqNum ] )

		    # build allele_grid_rows for the allele

		    header = None
		    for [ termKey, term, indent ] in self.alleleTerms[allele]:
			if termKey == None:
				isHeader = 1
				header = term
			else:
				isHeader = 0

			gridRowKey = getGridRowKey (allele, termKey, term,
				header)

			if self.ids.has_key (termKey):
				termID = self.ids[termKey]
			elif self.ids.has_key (term):
				termID = self.ids[term]
			else:
				termID = None	# should not happen

			# if we are annotated to a header term, and that
			# header term does not end with 'phenotype', then we
			# need to append that to our term

			if (not isHeader) and (term == header) and \
				(not term.endswith('phenotype')):
					term = term + ' phenotype'

			# if this is a header term, then we need to remove
			# any trailing 'phenotype' string

			elif isHeader and (header.endswith('phenotype')):
				term = term.replace (' phenotype', '')

			agrRows.append ( [ gridRowKey, allele, term, termID,
				isHeader, indent ] )

		    # build allele_grid_value rows for the allele

		    # (grid row, grid col) -> value
		    cells = {}

		    for genotype in genotypeKeys:
			gridColKey = getGridColumnKey (allele, genotype)

			if not self.genotypeData.has_key(genotype):
			    continue

			for annotation in self.genotypeData[genotype]:
			    header = annotation[3]
			    termKey = annotation[4]
			    term = annotation[5]
			    qualifier = annotation[6]

			    if qualifier != None:
				    qualifier = Gatherer.resolve (qualifier)

			    gridRowKey = getGridRowKey (allele, termKey, term,
				header)
			    headerRowKey = getGridRowKey(allele, None, header,
				header)

			    # what symbol will go in the cell?

			    if qualifier and qualifier.lower() == 'not':
				    symbol = NOT
			    elif qualifier and qualifier.lower() == 'normal':
				    symbol = NORMAL
			    else:
				    symbol = CHECK

			    # any annotations to the term 'normal phenotype'
			    # should be treated as if they have a normal
			    # qualifier

			    if term.lower() == 'normal phenotype':
				    symbol = NORMAL

			    # for term/genotype pairs with normal qualifiers,
			    # any non-normal annotation will take precedence

			    key = (gridRowKey, gridColKey)
			    headerKey = (headerRowKey, gridColKey)

			    for k in [ key, headerKey ]:
			        if not cells.has_key(k):
				    cells[k] = symbol

			        elif cells[k] in (NORMAL, NOT):
				    if symbol == CHECK:
					    cells[k] = symbol

		    for ((gridRowKey, gridColKey), symbol) in cells.items():
			agvRows.append ( [ gridRowKey, gridColKey, allele,
			    symbol ] )

		logger.debug ('Collected %d grid values for %d alleles' % (
			len(agvRows), len(alleleKeys) ) )

		agrRows = sortAlleleGenotypeRows (agrRows)

		# done compiling our grid; send to output

		self.output.append ( [ agcCols, agcRows ] )
		self.output.append ( [ agrCols, agrRows ] )
		self.output.append ( [ agvCols, agvRows ] )
		return

	def collateResults (self):

		# slice and dice the query results to get them into the forms
		# we need (stored in instance variables)

		self.cacheRefInfo()
		self.cacheGenotypesPerAllele()
		self.cacheEvidence()
		self.cacheAnnotations()
		self.addAnnotationIndents()
		self.sortGenotypesPerAllele()
		self.cacheTermsPerGenotype()
		self.cacheTermsPerAllele()
		self.cacheIDs()

		# now build the output lists

		self.buildGenotypeTables()
		self.buildPhenotypeTables()
		return


###--- globals ---###

cmds = [
	# 0. J: numbers for each reference used as annotation evidence
	'''select distinct ve._Refs_key,
		aa.numericPart,
		aa.accID
	from voc_evidence ve,
		acc_accession aa
	where ve._Refs_key = aa._Object_key
		and aa._MGIType_key = 1
		and aa.prefixPart = 'J:'
		and aa._LogicalDB_key = 1
		and aa.preferred = 1''',
			
	# 1. genotypes per allele, for those with annotations
	'''select distinct gag._Genotype_key,
		gag._Allele_key
	from gxd_allelegenotype gag,
		voc_annot va
	where va._Object_key = gag._Genotype_key
		and va._AnnotType_key in (1002, 1020)''',

	# 2. notes and refs for MP annotation evidence records:
	#	1008 : General
	#	1015 : Background Sensitivity
	#	1031 : Normal
	'''select e._Annot_key,
		e._AnnotEvidence_key,
		n._NoteType_key,
		n._Note_key,
		c.sequenceNum,
		e._Refs_key,
		c.note
	from voc_evidence e,
		mgi_note n,
		mgi_notechunk c
	where e._AnnotEvidence_key = n._Object_key
		and n._NoteType_key in (1008, 1015, 1031)
		and n._Note_key = c._Note_key
	order by e._Annot_key, e._AnnotEvidence_key, n._NoteType_key,
		n._Note_key, c.sequenceNum''',

	# 3. MP annotations per genotype; note that evidence terms are not
	# displayed in the front-end for MP annotations

# this misses annotations to header terms:
#	'''select distinct va._Object_key as _Genotype_key,
#		vah.sequenceNum,
#		ht.term as headerTerm, 
#		vt._Term_key, 
#		vt.term,
#		va._Annot_key,
#		va._Qualifier_key
#	from voc_annot va,
#		voc_term vt,
#		dag_closure dc,
#		voc_annotheader vah,
#		voc_term ht
#	where va._AnnotType_key = 1002
#		and va._Term_key = vt._Term_key
#		and dc._DescendentObject_key = va._Term_key
#		and dc._AncestorObject_key = vah._Term_key
#		and vah._Term_key = ht._Term_key
#		and va._Object_key = vah._Object_key
#	order by va._Object_key, vah.sequenceNum''',

	# top part of the 'union' gets annotations to non-header terms, while
	# the lower part gets annotations to header terms
	'''select gag._Allele_key,
		va._Object_key as _Genotype_key,
		vah.sequenceNum,
		ht.term as headerTerm, 
		vt._Term_key, 
		vt.term,
		va._Annot_key,
		va._Qualifier_key
	from voc_annot va,
		voc_term vt,
		dag_closure dc,
		voc_annotheader vah,
		voc_term ht,
		gxd_allelegenotype gag
	where va._AnnotType_key = 1002
	and gag._Genotype_key = va._Object_key
		and va._Term_key = vt._Term_key
		and dc._DescendentObject_key = va._Term_key
		and dc._AncestorObject_key = vah._Term_key
		and vah._Term_key = ht._Term_key
		and va._Object_key = vah._Object_key
	union
	select gag._Allele_key,
		va._Object_key as _Genotype_key,
		vah.sequenceNum,
		ht.term as headerTerm, 
		vt._Term_key, 
		vt.term,
		va._Annot_key,
		va._Qualifier_key
	from voc_annot va,
		voc_term vt,
		voc_annotheader vah,
		voc_term ht,
		gxd_allelegenotype gag
	where va._AnnotType_key = 1002
	and gag._Genotype_key = va._Object_key
		and va._Term_key = vt._Term_key
		and va._Term_key = vah._Term_key
		and vah._Term_key = ht._Term_key
		and va._Object_key = vah._Object_key
	order by _Allele_key, headerTerm, sequenceNum''',

	# 4. cache primary ID for each term annotated to a genotype
	'''select distinct v._Term_key,
		a._LogicalDB_key,
		a.accID
	from voc_annot v,
		acc_accession a
	where v._Term_key = a._Object_key
		and a._MGIType_key = 13
		and a.preferred = 1
		and v._AnnotType_key = 1002
	order by v._Term_key, a._LogicalDB_key''',

	# 5. cache primary ID for each MP header term
	'''select distinct v._Term_key,
		a._LogicalDB_key,
		a.accID,
		t.term
	from voc_annotheader v,
		acc_accession a,
		voc_term t
	where v._Term_key = a._Object_key
		and a._MGIType_key = 13
		and a.preferred = 1
		and v._AnnotType_key = 1002
		and v._Term_key = t._Term_key
	order by v._Term_key, a._LogicalDB_key''', 
	]

files = [
	('genotype_phenotype',
		[ 'genotypePhenotypeKey', 'genotypeKey', 'isHeaderTerm',
			'term', 'termID', 'indentationLevel',
			'referenceCount', 'noteCount', 'sequenceNum' ],
		'genotype_phenotype'),

	('genotype_phenotype_note',
		[ Gatherer.AUTO, 'genotypePhenotypeKey', 'note', 'jnumID',
			'sequenceNum' ],
		'genotype_phenotype_note'),

	('genotype_phenotype_reference',
		[ Gatherer.AUTO, 'genotypePhenotypeKey', 'referenceKey',
			'jnumID', 'sequenceNum' ],
		'genotype_phenotype_reference'),

	('allele_grid_column',
		[ 'gridColumnKey', 'alleleKey', 'alleleGenotypeKey',
			'genotypeType', 'genotypeDesignation', 'sequenceNum'],
		'allele_grid_column'),

	('allele_grid_row',
		[ 'gridRowKey', 'alleleKey', 'term', 'termID', 'isHeader',
			'indentationLevel', 'sequenceNum' ],
		'allele_grid_row'),

	('allele_grid_value',
		[ Gatherer.AUTO, 'gridRowKey', 'gridColumnKey', 'alleleKey',
			'value' ],
		'allele_grid_value'),
	]

# global instance of a PhenotypeGatherer
gatherer = PhenotypeGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
