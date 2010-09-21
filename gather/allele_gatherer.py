#!/usr/local/bin/python
# 
# gathers data for the 'allele' table in the front-end database

import Gatherer
import re
import logger

###--- Classes ---###

class AlleleGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the allele table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for alleles,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# extract driver data from the first query, and cache it
		# for later use in postprocessResults()

		keyCol = Gatherer.columnNumber (self.results[0][0],
			'_Allele_key')
		driverCol = Gatherer.columnNumber (self.results[0][0],
			'driverNote')

		self.driver = {}
		for row in self.results[0][1]:
			self.driver[row[keyCol]] = row[driverCol]

		logger.debug ('Found %d recombinase alleles' % \
			len(self.driver))

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

		logger.debug('Found %d molecular notes' % len(self.molNote))

		# extract the holder and company ID for deltagen/lexicon
		# knockouts, and cache them for postprocessResults()

		keyCol = Gatherer.columnNumber (self.results[4][0],
			'_Allele_key')
		holderCol = Gatherer.columnNumber (self.results[4][0],
			'holder')
		companyCol = Gatherer.columnNumber (self.results[4][0],
			'companyID')

		self.knockouts = {}

		for r in self.results[4][1]:
			self.knockouts[r[keyCol]] = (r[holderCol], 
				r[companyCol])

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

		# main results are in the last query

		self.finalColumns = self.results[-1][0]
		self.finalResults = self.results[-1][1]
		return

	def postprocessResults (self):
		self.convertFinalResultsToList()

		columns = self.finalColumns

		keyCol = Gatherer.columnNumber (columns, '_Allele_key')
		ldbCol = Gatherer.columnNumber (columns, '_LogicalDB_key')
		typeCol = Gatherer.columnNumber (columns, '_Allele_Type_key')
		symCol = Gatherer.columnNumber (columns, 'symbol')
		modeCol = Gatherer.columnNumber (columns, '_Mode_key')
		strainCol = Gatherer.columnNumber(columns, 'strain')
		transCol = Gatherer.columnNumber(columns, '_Transmission_key')

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

			self.addColumn('logicalDB', ldb, r, columns)
			self.addColumn('alleleType', alleleType, r, columns)
			self.addColumn('alleleSubType', None, r, columns)
			self.addColumn('onlyAlleleSymbol', symbol, r, columns)

			if self.driver.has_key (allele):
				isRecombinase = 1
				driver = self.driver[allele]
			else:
				isRecombinase = 0
				driver = None

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

			self.addColumn('isRecombinase', isRecombinase, r,
				columns)
			self.addColumn('driver', driver, r, columns)
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
				holder, company = self.knockouts[allele]
			else:
				holder = None
				company = None

			self.addColumn('holder', holder, r, columns)
			self.addColumn('companyID', company, r, columns)

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
		return

###--- globals ---###

cmds = [
	'''select distinct _Allele_key, driverNote from all_cre_cache''',

	'''select a._Allele_key, m.name
	from all_allele a, mrk_marker m
	where a._Marker_key = m._Marker_key''',

	'''select n._Object_key, c.note, c.sequenceNum
	from mgi_note n, mgi_notechunk c
	where n._Note_key = c._Note_key
		and n._NoteType_key = 1032
	order by c.sequenceNum''',

	'''select n._Object_key, c.note, c.sequenceNum
	from mgi_note n, mgi_notechunk c
	where n._Note_key = c._Note_key
		and n._NoteType_key = 1021
	order by c.sequenceNum''',

	'select _Allele_key, holder, companyID from ALL_Knockout_Cache',

	'''select a._Object_key as _Allele_key, p._Image_key
		from img_imagepane_assoc a,
			img_imagepane p
		where a.isPrimary = 1
			and a._ImagePane_key = p._ImagePane_key
			and a._MGIType_key = 11''',

	'''select a._Allele_key, a.symbol, a.name, a._Allele_Type_key,
		ac.accID, ac._LogicalDB_key, s.strain, a._Mode_key,
		a._Transmission_key
	from all_allele a, acc_accession ac, prb_strain s
	where a._Allele_key = ac._Object_key
		and ac._MGIType_key = 11
		and ac.preferred = 1 
		and a._Strain_key = s._Strain_key
		and ac.private = 0''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'_Allele_key', 'symbol', 'name', 'onlyAlleleSymbol', 'geneName',
	'accID', 'logicalDB', 'alleleType', 'alleleSubType',
	'isRecombinase', 'driver', 'inducibleNote', 'molecularDescription',
	'strain', 'strainLabel', 'inheritanceMode', 'holder', 'companyID',
	'transmission', 'transmission_phrase', 'imageKey',
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
