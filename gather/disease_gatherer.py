#!/usr/local/bin/python
# 
# gathers data for the 'genotype_disease', 'genotype_disease_reference',
# and 'allele_disease' tables in the front-end database

import Gatherer
import logger
import GenotypeClassifier
import symbolsort

###--- Constants ---###
CATEGORY3_HEADERS = {1:"Models with phenotypic similarity to human disease where etiologies involve orthologs.",
			2:"Models with phenotypic similarity to human disease where etiologies are distinct.",
			4:"Models involving transgenes or other mutation types.",
			5:"No similarity to the expected human disease phenotype was found."}

###--- Functions ---###

ADKEYS = {}
def getAlleleDiseaseKey (alleleKey, termID):
	# generate primary key for allele_disease table
	global ADKEYS

	parms = (alleleKey, termID)
	if not ADKEYS.has_key(parms):
		ADKEYS[parms] = len(ADKEYS)
	return ADKEYS[parms]

DGKEYS = {}
def getDiseaseGenotypeKey (alleleGenotypeKey, alleleDiseaseKey):
	# generate primary key for allele_disease_genotype table
	global DGKEYS

	parms = (alleleGenotypeKey, alleleDiseaseKey)
	if not DGKEYS.has_key(parms):
		DGKEYS[parms] = len(DGKEYS)
	return DGKEYS[parms]

GDKEYS = {}
def getGenotypeDiseaseKey (genotypeKey, isNot, termID,header=None):
	# generate primary key for genotype_disease table
	global GDKEYS

	parms = (genotypeKey, isNot, termID,header)
	if not GDKEYS.has_key(parms):
		GDKEYS[parms] = len(GDKEYS)
	return GDKEYS[parms]

GDNOTES = {}
def getDiseaseNoteNumber(genotypeKey,note):
	# retrieve note number for this genotype / note pair
	global GDNOTES
	notes = GDNOTES.setdefault(genotypeKey,{})
	if note not in notes:
		notes[note] = len(notes)+1
	return notes[note]

GDHEADERS = {}
def getHeader(category3,header,genotypeKey,term):
	global CATEGORY3_HEADERS
	global GDHEADERS
	key = (genotypeKey,term)
	if category3 not in CATEGORY3_HEADERS:
		return header
	# if we have a category3=2 but there alreay is a category3=1 for this genotype, turn it into a 1
	if category3==2:
	    if key in GDHEADERS \
		and 1 in GDHEADERS[key]:
			category3=1	
	GDHEADERS.setdefault(key,[]).append(category3)
	return CATEGORY3_HEADERS[category3]
			


###--- Classes ---###

class DiseaseGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the tables given above
	# Has: queries to execute against the source database
	# Does: queries the source database for disease annotations for
	#	genotypes, collates results, writes tab-delimited text file
	def buildDiseaseSorts(self):
		dSorts = {}
		(cols, rows) = self.results[0]
		termCol = Gatherer.columnNumber (cols, 'term')
		terms = []
		for row in rows:
			term = row[termCol]
			terms.append(term)
		terms.sort(symbolsort.nomenCompare)
		seq = 0
		for term in terms:
			seq += 1
			dSorts[term] = seq
		return dSorts

	def collateResults (self):
		dSorts = self.buildDiseaseSorts()
		(cols, rows) = self.results[0]

		# get column numbers for each field

		alleleCol = Gatherer.columnNumber (cols, '_Allele_key')
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier')
		termCol = Gatherer.columnNumber (cols, 'term')
		termKeyCol = Gatherer.columnNumber (cols, 'termKey')
		termIDCol = Gatherer.columnNumber (cols, 'termID')
		refsCol = Gatherer.columnNumber (cols, '_Refs_key')
		jnumCol = Gatherer.columnNumber (cols, 'jnumID')
		categoryCol = Gatherer.columnNumber (cols, 'omimCategory3')
		headerCol = Gatherer.columnNumber (cols, 'header')
		headerFootnoteCol = Gatherer.columnNumber (cols, 'headerFootnote')
		genotypeFootnoteCol = Gatherer.columnNumber (cols, 'genotypeFootnote')
		conditionalCol = Gatherer.columnNumber (cols, 'isConditional')

		# loop through the results to create build the output rows

		adData = {}		# allele_disease table
		gdData = {}		# genotype_disease table
		gdrData = {}		# genotype_disease_reference table
		gdfData = {}		# genotype_disease_footnote table

		i = 0			# counter of rows
		prevAllele = None	# allele key from previous row
		prevGenotype = None	# genotype key from previous row
		prevHeader = None	# header from previous row
		genotypeNotes = {}	# genotype footnote -> index
		alleleNotes = {}	# allele footnote -> index

		# footnote for conditional genotypes
		conditionalNote = 'Conditionally targeted allele(s).'

		for row in rows:
			i = i + 1	# update our row counter

			# bring the data into variables

			allele = row[alleleCol]
			genotype = row[genotypeCol]
			qualifier = row[qualifierCol]
			term = row[termCol]
			termKey = row[termKeyCol]
			termID = row[termIDCol]
			refsKey = row[refsCol]
			jnumID = row[jnumCol]
			category = row[categoryCol]
			header = row[headerCol]
			headerNote = row[headerFootnoteCol]
			genotypeNote = row[genotypeFootnoteCol]
			genotypeNoteNumber = None
			headerNoteNumber = None
			isConditional = row[conditionalCol]

			# HACK: for now we are translating the header note into a category3 header note
			header = getHeader(category,header,genotype,term)

			# look up the allele/genotype key from the
			# allele_to_genotype table

			alleleGenotype = \
				GenotypeClassifier.getAlleleGenotypeKey (
					allele, genotype)

			# the only qualifier we expect to see is 'not'

			if qualifier != None:
				isNot = 1
			else:
				isNot = 0

			# if the header has changed, we need to add a header
			# row to allele_disease and genotype_disease

			addGenotypeHeader = False
			addAlleleHeader = False

			if header and (header != prevHeader):
				addGenotypeHeader = True
				addAlleleHeader = True

			if allele != prevAllele:
				addGenotypeHeader = True
				addAlleleHeader = True
				genotypeNotes = {}
				alleleNotes = {}

			if genotype != prevGenotype:
				addGenotypeHeader = True
				genotypeNotes = {}

			# initial values for use in headers

			genotypeNoteNumber = None
			alleleNoteNumber = None

			if headerNote:
			    if not genotypeNotes.has_key (headerNote):
				    genotypeNotes[headerNote] = 1 + \
					len(genotypeNotes)
			    genotypeNoteNumber = getDiseaseNoteNumber(genotype,headerNote)

			    if not alleleNotes.has_key (headerNote):
				    alleleNotes[headerNote] = 1 + \
					len(alleleNotes)
			    alleleNoteNumber = alleleNotes[headerNote]
			    hasHeaderNote = 1
			else:
			    hasHeaderNote = 0

			if addGenotypeHeader:
				row = {
					'genotype' : genotype,
					'isHeading' : 1,
					'isNot' : 0,
					'term' : header,
					'termID' : None,
					'hasFootnote' : hasHeaderNote,
					'referenceCount' : 0,
					'sequenceNum' : len(gdData) + 1
					}
				genotypeDiseaseH = getGenotypeDiseaseKey (
					genotype, isNot, header)
				# NOTE: turned off loading of headers for now
				#if genotypeDiseaseH not in gdData:
				#	gdData[genotypeDiseaseH] = row
				i = i + 1

				#if headerNote:
				#	gdfData[i] = {
				#		'genotypeDisease' : \
				#			genotypeDiseaseH,
				#		'number' : genotypeNoteNumber,
				#		'note' : headerNote,
				#		}
				#	i = i + 1

			if addAlleleHeader:
				row = {
					'alleleKey' : allele,
					'isHeading' : 1,
					'isNot' : 0,
					'term' : header,
					'termID' : None,
					'hasFootnote' : hasHeaderNote,
					'sequenceNum' : len(adData) + 1,
					'by_alpha' : len(adData) + 1
					}
				alleleDiseaseH = getAlleleDiseaseKey (allele,
					header)
				adData[alleleDiseaseH] = row
				i = i + 1

			# look up keys for the allele_disease,
			# allele_disease_genotype, and genotype_disease tables

			alleleDisease = getAlleleDiseaseKey (allele, 
				termID)
			diseaseGenotype = getDiseaseGenotypeKey (
				alleleGenotype, alleleDisease)
			genotypeDisease = getGenotypeDiseaseKey (
				genotype, isNot, termID)

			# always need to add the reference rows for:
			# allele_disease_reference and
			# genotype_disease_reference

			if refsKey:
				gdrRow = {
					'genotypeDisease' : genotypeDisease,
					'referenceKey' : refsKey,
					'jnumID' : jnumID,
					'sequenceNum' : len(gdrData) + 1
					}
				gdrRowKey = (genotypeDisease,refsKey)
				gdrData[gdrRowKey] = gdrRow

			# look for genotype-based footnotes to add
			# (reset from variables used in headers above)

			genotypeNoteNumber = None
			alleleNoteNumber = None

			if genotypeNote:
			    if not genotypeNotes.has_key (genotypeNote):
				    genotypeNotes[genotypeNote] = 1 + \
					len(genotypeNotes)
			    genotypeNoteNumber = getDiseaseNoteNumber(genotype,genotypeNote)

			    if not alleleNotes.has_key (genotypeNote):
				    alleleNotes[genotypeNote] = 1 + \
					len(alleleNotes)
			    alleleNoteNumber = alleleNotes[genotypeNote]
			    hasFootnote = 1
			    hasAnyFootnote = 1
			else:
			    hasFootnote = 0
			    hasAnyFootnote = 0

			# if the genotype is flagged as conditional, we have
			# an extra footnote for that

			if isConditional:
			    if not genotypeNotes.has_key (conditionalNote):
				    genotypeNotes[conditionalNote] = 1 + \
					len(genotypeNotes)

			    if not alleleNotes.has_key (conditionalNote):
				    alleleNotes[conditionalNote] = 1 + \
					len(alleleNotes)
			    hasConditionalFootnote = 1
			    hasAnyFootnote = 1
			else:
			    hasConditionalFootnote = 0

			# update the diseases for a genotype table
			# (genotype_disease)

			if not gdData.has_key (genotypeDisease):
				row = {
					'genotype' : genotype,
					'isHeading' : 0,
					'isNot' : isNot,
					'term' : term,
					'termKey' : termKey,
					'termID' : termID,
					'hasFootnote' : hasAnyFootnote,
					'referenceCount' : 1,
					'sequenceNum' : dSorts[term]
					}
				gdData[genotypeDisease] = row

			else:
			    # just increment the reference count
			    gdData[genotypeDisease]['referenceCount'] = \
				gdData[genotypeDisease]['referenceCount'] + 1

			# add footnotes
			if hasFootnote:
				gdfData[i] = {
					'genotypeDisease' : \
						genotypeDisease,
					'number' : genotypeNoteNumber,
					'note' : genotypeNote,
					}
				i = i + 1

			if hasConditionalFootnote:
				conditionalNoteNumber = getDiseaseNoteNumber(genotype,conditionalNote)
				gdfData[i] = {
					'genotypeDisease' : \
						genotypeDisease,
					'number' : conditionalNoteNumber, 
					'note' : conditionalNote,
					}
				i = i + 1

			# add data for the allele_disease table

			if not adData.has_key (alleleDisease):
				row = {
					'alleleKey' : allele,
					'isHeading' : 0,
					'isNot' : isNot,
					'term' : term,
					'termID' : termID,
					'hasFootnote' : hasAnyFootnote,
					'sequenceNum' : len(adData) + 1,
					'by_alpha' : dSorts[term]
					}
				adData[alleleDisease] = row

			prevHeader = header
			prevAllele = allele
			prevGenotype = genotype

		# definition of genotype_disease table rows

		gdCols = [ 'genotypeDiseaseKey', 'genotypeKey',
			'isHeading', 'isNot', 'term', 'termID',
			'referenceCount', 'hasFootnote', 'sequenceNum' ]
		gdRows = []

		# definition of genotype_disease_reference table rows

		gdrCols = [ 'genotypeDiseaseKey', 'referenceKey', 'jnumID',
			'sequenceNum' ]
		gdrRows = []

		# definition of allele_disease table rows

		adCols = [ 'alleleDiseaseKey', 'alleleKey', 'isHeading',
			'isNot', 'term', 'termID', 
			'hasFootnote', 'sequenceNum','by_alpha' ]
		adRows = []

		# definition of allele_disease_genotype table rows

		adgCols = [ 'diseaseGenotypeKey', 'alleleGenotypeKey',
			'alleleDiseaseKey', 'sequenceNum' ]
		adgRows = []

		# definition of allele_disease_reference table rows

		adrCols = [ 'diseaseGenotypeKey', 'referenceKey', 'jnumID',
			'sequenceNum' ]
		adrRows = []

		# definition of allele_disease_footnote table rows

		adfCols = [ 'alleleDiseaseKey', 'number', 'note' ]
		adfRows = []

		# definition of genotype_disease_footnote table rows

		gdfCols = [ 'genotypeDiseaseKey', 'number', 'note' ]
		gdfRows = []

		# compile our various data rows...

		genotypeDiseaseKeys = gdData.keys()
		genotypeDiseaseKeys.sort()

		for genotypeDisease in genotypeDiseaseKeys:
			r = gdData[genotypeDisease]
			gdRows.append ( [ genotypeDisease,
				r['genotype'],r['isHeading'],
				r['isNot'], r['term'], r['termKey'], r['termID'],
				r['referenceCount'], r['hasFootnote'],
				r['sequenceNum'] ] )

		self.output.append ( (gdCols, gdRows) )
		logger.debug ('Compiled %d genotype_disease rows' % \
			len(gdRows))

		countedKeys = gdrData.keys()
		countedKeys.sort()

		for key in countedKeys:
			r = gdrData[key]
			gdrRows.append ( [ r['genotypeDisease'],
				r['referenceKey'], r['jnumID'],
				r['sequenceNum'] ] )

		self.output.append ( (gdrCols, gdrRows) )
		logger.debug ('Compiled %d genotype_disease_reference rows' %\
			len(gdrRows))

		alleleDiseaseKeys = adData.keys()
		alleleDiseaseKeys.sort()

		for alleleDisease in alleleDiseaseKeys:
			r = adData[alleleDisease]
			adRows.append ( [ alleleDisease,
				r['alleleKey'], r['isHeading'], r['isNot'],
				r['term'], r['termID'], 
				r['hasFootnote'], r['sequenceNum'],r['by_alpha'] ] )

		self.output.append ( (adCols, adRows) )
		logger.debug ('Compiled %d allele_disease rows' % len(adRows))

		return

###--- globals ---###

cmds = [
	'''select distinct a._Allele_key,
		g._Genotype_key,
		o.qualifier,
		o.term,
		o.term_key as termKey,
		o.termID,
		o._Refs_key,
		o.jnumID,
		o.omimCategory3,
		o.header,
		o.headerFootnote,
		o.genotypeFootnote,
		t.isConditional
	from mrk_do_cache o,
		gxd_allelegenotype g, 
		all_allele a,
		gxd_genotype t
	where a._Allele_key = g._Allele_key
		and g._Genotype_key = o._Genotype_key
		and a._Marker_key = o._Marker_key
		and g._Genotype_key = t._Genotype_key
	order by a._Allele_key, o.omimCategory3, o.term, g._Genotype_key'''
	]

files = [
	('genotype_disease',
		[ 'genotypeDiseaseKey', 'genotypeKey','isHeading',
			'isNot', 'term', 'termKey', 'termID', 'referenceCount',
			'hasFootnote', 'sequenceNum' ],
		'genotype_disease'),

	('genotype_disease_reference',
		[ Gatherer.AUTO, 'genotypeDiseaseKey', 'referenceKey',
			'jnumID', 'sequenceNum' ],
		'genotype_disease_reference'),

	('allele_disease',
		[ 'alleleDiseaseKey', 'alleleKey', 'isHeading', 'isNot',
			'term', 'termID', 'hasFootnote',
			'sequenceNum','by_alpha' ],
		'allele_disease'),
	]

# global instance of a DiseaseGatherer
gatherer = DiseaseGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
