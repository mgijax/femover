#!/usr/local/bin/python
# 
# gathers data for the 'allele' table in the front-end database

import Gatherer
import re
import logger

###--- Constants ---###
NOT_SPECIFIED="Not Specified"
ALLELE_SUBTYPE_ANNOT_KEY=1014

###--- Classes ---###

class AlleleGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the allele table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for alleles,
	#	collates results, writes tab-delimited text file

	# maps allele_key to chromosome
	def initChromosomeLookup(self):
		chrLookup={}
		for r in self.results[8][1]:
			chrLookup[r[0]]=r[1]
		return chrLookup

	def buildAlleleSubTypes(self):
		stLookup={}
		for r in self.results[9][1]:
			stLookup.setdefault(r[0],[]).append(r[1])
		return stLookup

	def collateResults (self):
		# extract driver data from the first query, and cache it
		# for later use in postprocessResults()

		keyCol = Gatherer.columnNumber (self.results[0][0], '_Allele_key')
		driverCol = Gatherer.columnNumber (self.results[0][0], 'driverNote')
		driverKeyCol = Gatherer.columnNumber (self.results[0][0], '_Marker_key')

		self.driver = {}				# allele key : (driver symbol, key)
		for row in self.results[0][1]:
			self.driver[row[keyCol]] = (row[driverCol], row[driverKeyCol])

		logger.debug ('Found %d recombinase alleles' % len(self.driver))

		# extract gene name data from the second query and cache it
		# for later use in postprocessResults()

		keyCol = Gatherer.columnNumber (self.results[1][0],
			'_Allele_key')
		nameCol = Gatherer.columnNumber (self.results[1][0], 'name')

		self.genes = {}
		for row in self.results[1][1]:
			self.genes[row[keyCol]] = row[nameCol]

		logger.debug ('Found %d gene names' % len(self.genes))

		# extract inducible notes from the third query and cache them
		# for later use in postprocessResults()

		keyCol = Gatherer.columnNumber (self.results[2][0],
			'_Object_key')
		noteCol = Gatherer.columnNumber (self.results[2][0], 'note')

		self.inducible = {}
		for row in self.results[2][1]:
			key = row[keyCol]
			if self.inducible.has_key(key):
				self.inducible[key] = self.inducible[key] + \
					row[noteCol]
			else:
				self.inducible[key] = row[noteCol]

		# trim trailing whitespace

		for key in self.inducible.keys():
			self.inducible[key] = self.inducible[key].rstrip()

		logger.debug('Found %d inducible notes' % len(self.inducible))

		# extract molecular description from the fourth query and
		# cache them for later use in postprocessResults()

		keyCol = Gatherer.columnNumber (self.results[3][0],
			'_Object_key')
		noteCol = Gatherer.columnNumber (self.results[3][0], 'note')

		self.molNote = {}
		for row in self.results[3][1]:
			key = row[keyCol]
			if self.molNote.has_key(key):
				self.molNote[key] = self.molNote[key] + \
					row[noteCol]
			else:
				self.molNote[key] = row[noteCol]

		# trim trailing whitespace

		for key in self.molNote.keys():
			self.molNote[key] = self.molNote[key].rstrip()

		logger.debug('Found %d molecular notes' % len(self.molNote))

		# extract the holder and company ID for deltagen/lexicon
		# knockouts, and cache them for postprocessResults()

		keyCol = Gatherer.columnNumber (self.results[4][0], '_Allele_key')
		holderCol = Gatherer.columnNumber (self.results[4][0], 'holder')
		companyCol = Gatherer.columnNumber (self.results[4][0], 'companyID')
		repositoryCol = Gatherer.columnNumber (self.results[4][0], 'repository')
		jrsIDCol = Gatherer.columnNumber (self.results[4][0], 'jrsID')

		self.knockouts = {}

		for r in self.results[4][1]:
			self.knockouts[r[keyCol]] = (r[holderCol], r[companyCol], r[repositoryCol], r[jrsIDCol])

		logger.debug ('Found %d knockouts' % len(self.knockouts))

		# extract the primary image for each allele (where available)
		# and cache them for postprocessResults()

		keyCol = Gatherer.columnNumber (self.results[5][0],
			'_Allele_key')
		imageCol = Gatherer.columnNumber (self.results[5][0],
			'_Image_key')

		self.images = {}

		for r in self.results[5][1]:
			self.images[r[keyCol]] = r[imageCol]

		logger.debug ('Found %d primary images' % len(self.images))

		# get the set of alleles which have disease model genotypes

		cols, rows = self.results[6]
		keyCol = Gatherer.columnNumber (cols, '_Allele_key')

		self.diseaseModels = {}
		for r in rows:
			self.diseaseModels[r[keyCol]] = 1

		logger.debug ('Found %d disease model alleles' % \
			len(self.diseaseModels))

		# main results are in the last query
		self.finalColumns = self.results[7][0]
		self.finalResults = self.results[7][1]
		return

	def postprocessResults (self):
		# get a lookup of allele_key->chromosome
		chrLookup=self.initChromosomeLookup()
		subTypeLookup=self.buildAlleleSubTypes()

		self.convertFinalResultsToList()

		columns = self.finalColumns

		keyCol = Gatherer.columnNumber (columns, '_Allele_key')
		ldbCol = Gatherer.columnNumber (columns, '_LogicalDB_key')
		typeCol = Gatherer.columnNumber (columns, '_Allele_Type_key')
		symCol = Gatherer.columnNumber (columns, 'symbol')
		modeCol = Gatherer.columnNumber (columns, '_Mode_key')
		strainCol = Gatherer.columnNumber(columns, 'strain')
		transCol = Gatherer.columnNumber(columns, '_Transmission_key')
		collCol = Gatherer.columnNumber(columns, 'collxn')

		# pulls the actual allele symbol out of the combined
		# marker symbol<allele symbol> field
		symFinder = re.compile ('<([^>]*)>')

		for r in self.finalResults:
			alleleType = Gatherer.resolve (r[typeCol])

			allele = r[keyCol]

			ldb = Gatherer.resolve (r[ldbCol], 'acc_logicaldb',
				'_LogicalDB_key', 'name')

			match = symFinder.search(r[symCol])
			if match:
				symbol = match.group(1)
			else:
				symbol = r[symCol]

			
			subTypes=""
			if allele in subTypeLookup:
				subTypeLookup[allele].sort()
				subTypes=", ".join(subTypeLookup[allele])
			chr=allele in chrLookup and chrLookup[allele] or None

			collection=r[collCol]
			if collection==NOT_SPECIFIED:
				collection=None

			self.addColumn('logicalDB', ldb, r, columns)
			self.addColumn('alleleType', alleleType, r, columns)
			self.addColumn('alleleSubType', subTypes, r, columns)
			self.addColumn('chromosome', chr, r, columns)
			self.addColumn('collection', collection, r, columns)
			self.addColumn('onlyAlleleSymbol', symbol, r, columns)

			if self.driver.has_key (allele):
				isRecombinase = 1
				driver, driverKey = self.driver[allele]
			else:
				isRecombinase = 0
				driver = None
				driverKey = None

			if self.genes.has_key (allele):
				gene = self.genes[allele]
			else:
				gene = None

			if self.inducible.has_key(allele):
				inducibleNote = self.inducible[allele]
			else:
				inducibleNote = None

			if self.molNote.has_key(allele):
				molNote = self.molNote[allele]
			else:
				molNote = None

			if self.diseaseModels.has_key(allele):
				diseaseModel = 1
			else:
				diseaseModel = 0

			self.addColumn('isRecombinase', isRecombinase, r,
				columns)
			self.addColumn('driver', driver, r, columns)
			self.addColumn('driver_key', driverKey, r, columns)
			self.addColumn('geneName', gene, r, columns)
			self.addColumn('inducibleNote', inducibleNote, r,
				columns)
			self.addColumn('molecularDescription', molNote, r,
				columns)

			if alleleType == 'QTL':
				strainLabel = 'Strain of Specimen'
			else:
				strainLabel = 'Strain of Origin'

			self.addColumn('strainLabel', strainLabel, r, columns)
			self.addColumn('inheritanceMode', Gatherer.resolve (
				r[modeCol]), r, columns)

			if self.knockouts.has_key(allele):
				holder, company, repository, jrsID = self.knockouts[allele]
			else:
				holder = None
				company = None
				repository = None
				jrsID = None

			self.addColumn('holder', holder, r, columns)
			self.addColumn('companyID', company, r, columns)
			self.addColumn('repository', repository, r, columns)
			self.addColumn('jrsID', jrsID, r, columns)

			self.addColumn('transmission', Gatherer.resolve (
				r[transCol]), r, columns)
			self.addColumn('transmission_phrase',
				Gatherer.resolve (r[transCol], 'voc_term',
				'_Term_key', 'abbreviation'), r, columns)

			if self.images.has_key(allele):
				image = self.images[allele]
			else:
				image = None

			self.addColumn('imageKey', image, r, columns)
			self.addColumn('hasDiseaseModel', diseaseModel, r,
				columns)
		return

###--- globals ---###

cmds = [
	# 0. Cre drivers
	'''select a._Allele_key, m._Marker_key, m.symbol as driverNote
		from mgi_relationship r, all_allele a, mrk_marker m
		where r._Category_key = 1006
		and r._Object_key_1 = a._Allele_key
		and r._Object_key_2 = m._Marker_key''',

	# 1. marker name for each allele
	'''select a._Allele_key, m.name
	from all_allele a, mrk_marker m
	where a._Marker_key = m._Marker_key''',

	# 2. Inducible allele notes
	'''select n._Object_key, c.note, c.sequenceNum
	from mgi_note n, mgi_notechunk c
	where n._Note_key = c._Note_key
		and n._NoteType_key = 1032
	order by c.sequenceNum''',

	# 3. Molecular allele notes
	'''select n._Object_key, c.note, c.sequenceNum
	from mgi_note n, mgi_notechunk c
	where n._Note_key = c._Note_key
		and n._NoteType_key = 1021
	order by c.sequenceNum''',

	# 4. Deltagen/Lexicon Knockout data
	'select _Allele_key, holder, companyID, repository, jrsID from ALL_Knockout_Cache',

	# 5. alleles' primary images
	'''select a._Object_key as _Allele_key, p._Image_key
		from img_imagepane_assoc a,
			img_imagepane p
		where a.isPrimary = 1
			and a._ImagePane_key = p._ImagePane_key
			and a._MGIType_key = 11''',

	# 6. alleles which have disease model genotypes
	'''select distinct gag._Allele_key
	from gxd_allelegenotype gag, voc_annot va
	where va._AnnotType_key = 1020
		and va._Object_key = gag._Genotype_key''',

	# 7. assume all alleles have an MGI ID
	'''select a._Allele_key, a.symbol, a.name, a._Allele_Type_key,
		ac.accID, ac._LogicalDB_key, s.strain, a._Mode_key,
		a._Transmission_key, a.isWildType, a.isMixed, coll.term collxn
	from all_allele a, acc_accession ac, prb_strain s,voc_term coll
	where a._Allele_key = ac._Object_key
		and ac._MGIType_key = 11
		and ac.preferred = 1 
		and ac._LogicalDB_key = 1
		and a._Strain_key = s._Strain_key
		and ac.private = 0
		and coll._term_key=a._collection_key''',

	# 8. map allele_key to chromosome
	'''
	select aa._allele_key, m.chromosome 
	from all_allele aa join 
	mrk_marker m on m._marker_key=aa._marker_key
	''',
	# 9. map allele_key to allele subtypes
	'''
	select va._object_key allele_key, st.term subtype
	from voc_annot va  join
		voc_term st on va._term_key=st._term_key
	where va._annottype_key=%s
	'''%ALLELE_SUBTYPE_ANNOT_KEY,
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'_Allele_key', 'symbol', 'name', 'onlyAlleleSymbol', 'geneName',
	'accID', 'logicalDB', 'alleleType', 'alleleSubType','chromosome','collection',
	'isRecombinase', 'isWildType', 'isMixed', 'driver', 'driver_key', 'inducibleNote',
	'molecularDescription', 'strain', 'strainLabel', 'inheritanceMode',
	'holder', 'companyID', 'transmission', 'transmission_phrase',
	'imageKey', 'hasDiseaseModel', 'repository', 'jrsID',
	]

# prefix for the filename of the output file
filenamePrefix = 'allele'

# global instance of a AlleleGatherer
gatherer = AlleleGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
