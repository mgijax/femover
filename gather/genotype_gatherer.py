#!/usr/local/bin/python
# 
# gathers data for the 'genotype' table in the front-end database

import Gatherer
import logger
import TagConverter
import GenotypeClassifier

###--- Classes ---###

class GenotypeGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the genotype table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for genotypes,
	#	collates results, writes tab-delimited text file

	def collateResults (self):

		# final query has the basic genotype data; we will start there
		# and augment it

		self.finalColumns = self.results[-1][0]
		self.finalResults = self.results[-1][1]
		self.convertFinalResultsToList()

		# gather which genotypes have primary images

		cols, rows = self.results[2]
		genotypeCol = Gatherer.columnNumber (cols, '_Object_key')
		hasImage = {}

		for row in rows:
			hasImage[row[genotypeCol]] = 1

		logger.debug ('Found %d genotypes with images' % \
			len(hasImage)) 

		# The two earlier queries get the allele composition strings
		# that need to be added for each genotype

		combos = [ (0, 'combo1'), (1, 'combo2') ]

		for (index, id) in combos:
			dict = {}		# genotype key -> note
			(cols, rows) = self.results[index]
			keyCol = Gatherer.columnNumber (cols, '_Genotype_key')
			noteCol = Gatherer.columnNumber (cols, 'note')

			for row in rows:
				key = row[keyCol]
				if dict.has_key(key):
					dict[key] = dict[key] + row[noteCol]
				else:
					dict[key] = row[noteCol]

			logger.debug ('Found %d %s notes' % (len(dict), id))

			keyCol = Gatherer.columnNumber (self.finalColumns,
					'_Genotype_key')
			for row in self.finalResults:
				key = row[keyCol]
				if dict.has_key(key):
					combo = TagConverter.convert (
						dict[key])
				else:
					combo = None

				self.addColumn (id, combo, row,
					self.finalColumns)

			logger.debug ('Mapped %d %s notes' % (len(dict), id))

		# handle the image and disease data, and the classification

		keyCol = Gatherer.columnNumber (self.finalColumns,
			'_Genotype_key')
		for row in self.finalResults:
			key = row[keyCol]

			if hasImage.has_key(key):
				image = 1
			else:
				image = 0

			disease = GenotypeClassifier.isDiseaseModel(key)
			pheno = GenotypeClassifier.hasPhenoData(key)

			self.addColumn ('hasImage', image, row,
				self.finalColumns)
			self.addColumn ('hasPhenoData', pheno, row,
				self.finalColumns)
			self.addColumn ('hasDiseaseModel', disease, row,
				self.finalColumns)
			self.addColumn ('classification',
				GenotypeClassifier.getClass (key),
				row, self.finalColumns) 
		return

###--- globals ---###

noteCmd = '''select mn._Object_key as _Genotype_key,
			mnc.note
		from mgi_note mn,
			mgi_notechunk mnc,
			mgi_notetype mnt
		where mn._MGIType_key = 12
			and mn._NoteType_key = mnt._NoteType_key
			and mnt.noteType = '%s'
			and mn._Note_key = mnc._Note_key
		order by mn._Object_key, mnc.sequenceNum'''
cmds = [
	# 0. "Combination Type 3" notes for use on allele summary page
	noteCmd % 'Combination Type 3',

	# 1. "Combination Type 2" notes for use on allele detail page
	noteCmd % 'Combination Type 2',

	# 2. get which genotypes have primary images associated
	'''select distinct _Object_key
		from img_imagepane_assoc
		where _MGIType_key = 12
		and isPrimary = 1''',

	# 3. assumes that a genotype has only one ID, that it is from MGI, and
	# that it is both preferred and non-private
	'''select distinct g._Genotype_key, s.strain, a.accID, g.note,
			g.isConditional
		from gxd_genotype g, acc_accession a, prb_strain s
		where g._Strain_key = s._Strain_key
			and g._Genotype_key = a._Object_key
			and a._MGIType_key = 12
			and a._LogicalDB_key = 1
			and a.preferred = 1''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Genotype_key', 'strain', 'accID', 'isConditional', 'note',
	'combo1', 'combo2', 'hasImage', 'hasPhenoData', 'hasDiseaseModel',
	'classification' ]

# prefix for the filename of the output file
filenamePrefix = 'genotype'

# global instance of a GenotypeGatherer
gatherer = GenotypeGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
