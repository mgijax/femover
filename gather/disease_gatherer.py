#!/usr/local/bin/python
# 
# gathers data for the 'genotype_disease', 'genotype_disease_reference',
# 'allele_disease', 'allele_disease_genotype', and 'allele_disease_reference'
# tables in the front-end database

import Gatherer
import logger
import GenotypeClassifier

###--- Functions ---###

ADKEYS = {}
def getAlleleDiseaseKey (alleleKey, isNot, termID):
	# generate primary key for allele_disease table
	global ADKEYS

	parms = (alleleKey, isNot, termID)
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
def getGenotypeDiseaseKey (alleleGenotypeKey, isNot, termID):
	# generate primary key for genotype_disease table
	global GDKEYS

	parms = (alleleGenotypeKey, isNot, termID)
	if not GDKEYS.has_key(parms):
		GDKEYS[parms] = len(GDKEYS)
	return GDKEYS[parms]


###--- Classes ---###

class DiseaseGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the tables given above
	# Has: queries to execute against the source database
	# Does: queries the source database for disease annotations for
	#	genotypes, collates results, writes tab-delimited text file

	def collateResults (self):
		(cols, rows) = self.results[0]

		# get column numbers for each field

		alleleCol = Gatherer.columnNumber (cols, '_Allele_key')
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier')
		termCol = Gatherer.columnNumber (cols, 'term')
		termIDCol = Gatherer.columnNumber (cols, 'termID')
		refsCol = Gatherer.columnNumber (cols, '_Refs_key')
		jnumCol = Gatherer.columnNumber (cols, 'jnumID')
		categoryCol = Gatherer.columnNumber (cols, 'omimCategory1')
		headerCol = Gatherer.columnNumber (cols, 'header')
		headerFootnoteCol = Gatherer.columnNumber (cols,
			'headerFootnote')
		genotypeFootnoteCol = Gatherer.columnNumber (cols,
			'genotypeFootnote')

		# loop through the results to create build the output rows

		adData = {}		# allele_disease table
		adgData = {}		# allele_disease_genotype table
		adrData = {}		# allele_disease_reference table
		gdData = {}		# genotype_disease table
		gdrData = {}		# genotype_disease_reference table

		i = 0			# counter of rows
		prevAllele = None	# allele key from previous row
		prevGenotype = None	# genotype key from previous row
		prevHeader = None	# header from previous row
		genotypeNotes = {}	# genotype footnote -> index
		alleleNotes = {}	# allele footnote -> index

		for row in rows:
			i = i + 1	# update our row counter

			# bring the data into variables

			allele = row[alleleCol]
			genotype = row[genotypeCol]
			qualifier = row[qualifierCol]
			term = row[termCol]
			termID = row[termIDCol]
			refsKey = row[refsCol]
			jnumID = row[jnumCol]
			category = row[categoryCol]
			header = row[headerCol]
			headerNote = row[headerFootnoteCol]
			genotypeNote = row[genotypeFootnoteCol]
			genotypeNoteNumber = None
			headerNoteNumber = None

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
			    genotypeNoteNumber = genotypeNotes[headerNote]

			    if not alleleNotes.has_key (headerNote):
				    alleleNotes[headerNote] = 1 + \
					len(alleleNotes)
			    alleleNoteNumber = alleleNotes[headerNote]

			    if allele == 3477:
				    logger.debug ('genotypeNotes: %s' % str(genotypeNotes))
				    logger.debug ('alleleNotes: %s' % str(alleleNotes))
				    logger.debug ('genotypeNoteNumber: %s' % genotypeNoteNumber)
				    logger.debug ('alleleNoteNumber: %s' % alleleNoteNumber)

			if addGenotypeHeader:
				row = {
					'alleleGenotype' : alleleGenotype,
					'isHeading' : 1,
					'isNot' : 0,
					'term' : header,
					'termID' : None,
					'footnoteNumber' : genotypeNoteNumber,
					'footnote' : headerNote,
					'referenceCount' : 0,
					'sequenceNum' : len(gdData) + 1
					}
				genotypeDiseaseH = getGenotypeDiseaseKey (
					alleleGenotype, isNot, header)
				gdData[genotypeDiseaseH] = row
				i = i + 1

			if addAlleleHeader:
				row = {
					'alleleKey' : allele,
					'isHeading' : 1,
					'isNot' : 0,
					'term' : header,
					'termID' : None,
					'footnoteNumber' : alleleNoteNumber,
					'footnote' : headerNote,
					'genotypeCount' : 0,
					'sequenceNum' : len(adData) + 1
					}
				alleleDiseaseH = getAlleleDiseaseKey (allele,
					isNot, header)
				adData[alleleDiseaseH] = row
				i = i + 1

			# look up keys for the allele_disease,
			# allele_disease_genotype, and genotype_disease tables

			alleleDisease = getAlleleDiseaseKey (allele, isNot,
				termID)
			diseaseGenotype = getDiseaseGenotypeKey (
				alleleGenotype, alleleDisease)
			genotypeDisease = getGenotypeDiseaseKey (
				alleleGenotype, isNot, termID)

			# always need to add the reference rows for:
			# allele_disease_reference and
			# genotype_disease_reference

			if refsKey:
				adrRow = {
					'diseaseGenotype' : diseaseGenotype,
					'referenceKey' : refsKey,
					'jnumID' : jnumID,
					'sequenceNum' : len(adrData) + 1
					}
				adrData[i] = adrRow

				gdrRow = {
					'genotypeDisease' : genotypeDisease,
					'referenceKey' : refsKey,
					'jnumID' : jnumID,
					'sequenceNum' : len(gdrData) + 1
					}
				gdrData[i] = gdrRow

			# look for genotype-based footnotes to add
			# (reset from variables used in headers above)

			genotypeNoteNumber = None
			alleleNoteNumber = None

			if genotypeNote:
			    if not genotypeNotes.has_key (genotypeNote):
				    genotypeNotes[genotypeNote] = 1 + \
					len(genotypeNotes)
			    genotypeNoteNumber = genotypeNotes[genotypeNote]

			    if not alleleNotes.has_key (genotypeNote):
				    alleleNotes[genotypeNote] = 1 + \
					len(alleleNotes)
			    alleleNoteNumber = alleleNotes[genotypeNote]

			# update the diseases for a genotype table
			# (genotype_disease)

			if not gdData.has_key (genotypeDisease):
				row = {
					'alleleGenotype' : alleleGenotype,
					'isHeading' : 0,
					'isNot' : isNot,
					'term' : term,
					'termID' : termID,
					'footnoteNumber' : genotypeNoteNumber,
					'footnote' : genotypeNote,
					'referenceCount' : 1,
					'sequenceNum' : len(gdData) + 1
					}
				gdData[genotypeDisease] = row
			else:
			    # just increment the reference count
			    gdData[genotypeDisease]['referenceCount'] = \
				gdData[genotypeDisease]['referenceCount'] + 1

			# add data for the allele_disease_genotype table

			if not adgData.has_key (diseaseGenotype):
				row = {
					'alleleGenotype' : alleleGenotype,
					'alleleDisease' : alleleDisease,
					'sequenceNum' : len(adgData) + 1
					}
				adgData[diseaseGenotype] = row
				addedGenotype = True
			else:
				addedGenotype = False

			# add data for the allele_disease table

			if not adData.has_key (alleleDisease):
				row = {
					'alleleKey' : allele,
					'isHeading' : 0,
					'isNot' : isNot,
					'term' : term,
					'termID' : termID,
					'footnoteNumber' : alleleNoteNumber,
					'footnote' : genotypeNote,
					'genotypeCount' : 1,
					'sequenceNum' : len(adData) + 1
					}
				adData[alleleDisease] = row
			elif addedGenotype:
			    # just increment the genotype count
			    adData[alleleDisease]['genotypeCount'] = \
				adData[alleleDisease]['genotypeCount'] + 1

			prevHeader = header
			prevAllele = allele
			prevGenotype = genotype

		# definition of genotype_disease table rows

		gdCols = [ 'genotypeDiseaseKey', 'alleleGenotypeKey',
			'isHeading', 'isNot', 'term', 'termID',
			'footnoteNumber', 'footnote', 'referenceCount',
			'sequenceNum' ]
		gdRows = []

		# definition of genotype_disease_reference table rows

		gdrCols = [ 'genotypeDiseaseKey', 'referenceKey', 'jnumID',
			'sequenceNum' ]
		gdrRows = []

		# definition of allele_disease table rows

		adCols = [ 'alleleDiseaseKey', 'alleleKey', 'isHeading',
			'isNot', 'term', 'termID', 'footnoteNumber',
			'footnote', 'genotypeCount', 'sequenceNum' ]
		adRows = []

		# definition of allele_disease_genotype table rows

		adgCols = [ 'diseaseGenotypeKey', 'alleleGenotypeKey',
			'alleleDiseaseKey', 'sequenceNum' ]
		adgRows = []

		# definition of allele_disease_reference table rows

		adrCols = [ 'diseaseGenotypeKey', 'referenceKey', 'jnumID',
			'sequenceNum' ]
		adrRows = []

		# compile our various data rows...

		genotypeDiseaseKeys = gdData.keys()
		genotypeDiseaseKeys.sort()

		for genotypeDisease in genotypeDiseaseKeys:
			r = gdData[genotypeDisease]
			gdRows.append ( [ genotypeDisease,
				r['alleleGenotype'], r['isHeading'],
				r['isNot'], r['term'], r['termID'],
				r['footnoteNumber'], r['footnote'],
				r['referenceCount'], r['sequenceNum'] ] )

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
				r['term'], r['termID'], r['footnoteNumber'],
				r['footnote'], r['genotypeCount'],
				r['sequenceNum'] ] )

		self.output.append ( (adCols, adRows) )
		logger.debug ('Compiled %d allele_disease rows' % len(adRows))

		diseaseGenotypeKeys = adgData.keys()
		diseaseGenotypeKeys.sort()

		for diseaseGenotype in diseaseGenotypeKeys:
			r = adgData[diseaseGenotype]
			adgRows.append ( [ diseaseGenotype,
				r['alleleGenotype'], r['alleleDisease'],
				r['sequenceNum'] ] )

		self.output.append ( (adgCols, adgRows) )
		logger.debug ('Compiled %d allele_disease_genotype rows' % \
			len(adgRows))

		countedKeys = adrData.keys()
		countedKeys.sort()

		for key in countedKeys:
			r = adrData[key]
			adrRows.append ( [ r['diseaseGenotype'],
				r['referenceKey'], r['jnumID'],
				r['sequenceNum'] ] )

		self.output.append ( (adrCols, adrRows) )
		logger.debug ('Compiled %d allele_disease_reference rows' % \
			len(adrRows))
		return

###--- globals ---###

cmds = [
	'''select distinct a._Allele_key,
		g._Genotype_key,
		o.qualifier,
		o.term,
		o.termID,
		o._Refs_key,
		o.jnumID,
		o.omimCategory1,
		o.header,
		o.headerFootnote,
		o.genotypeFootnote
	from mrk_omim_cache o,
		gxd_allelegenotype g, 
		all_allele a
	where a._Allele_key = g._Allele_key
		and g._Genotype_key = o._Genotype_key
		and a._Marker_key = o._Marker_key
		and o.omimCategory1 > -1
	order by a._Allele_key, o.omimCategory1, o.term, g._Genotype_key'''
	]

files = [
	('genotype_disease',
		[ 'genotypeDiseaseKey', 'alleleGenotypeKey', 'isHeading',
			'isNot', 'term', 'termID', 'footnoteNumber',
			'footnote', 'referenceCount', 'sequenceNum' ],
		'genotype_disease'),

	('genotype_disease_reference',
		[ Gatherer.AUTO, 'genotypeDiseaseKey', 'referenceKey',
			'jnumID', 'sequenceNum' ],
		'genotype_disease_reference'),

	('allele_disease',
		[ 'alleleDiseaseKey', 'alleleKey', 'isHeading', 'isNot',
			'term', 'termID', 'footnoteNumber', 'footnote',
			'genotypeCount', 'sequenceNum' ],
		'allele_disease'),

	('allele_disease_genotype',
		[ 'diseaseGenotypeKey', 'alleleGenotypeKey',
			'alleleDiseaseKey', 'sequenceNum' ],
		'allele_disease_genotype'),

	('allele_disease_reference',
		[ Gatherer.AUTO, 'diseaseGenotypeKey', 'referenceKey',
			'jnumID', 'sequenceNum' ],
		'allele_disease_reference'),
	]

# global instance of a DiseaseGatherer
gatherer = DiseaseGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
