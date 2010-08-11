#!/usr/local/bin/python
# 
# gathers data for the allele_recombinase_assay_result table in the front-end
# database

import Gatherer
import logger

###--- Classes ---###

class AlleleRecombinaseAssayResultGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the allele_recombinase_assay_result table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for recombinase assays,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		alleleComp = {}	# alleles[genotype key] = allelic composition
		strain = {}	# strain[genotype key] = strain
		assayNote = {}	# assayNote[assay key] = assay note

		# extract allelic composition and background strain from
		# query 0

		keyCol = Gatherer.columnNumber (self.results[0][0],
			'_Genotype_key')
		noteCol = Gatherer.columnNumber (self.results[0][0], 'note')
		strainCol = Gatherer.columnNumber (self.results[0][0],
			'strain')

		for r in self.results[0][1]:
			key = r[keyCol]
			if alleleComp.has_key (key):
				alleleComp[key] = alleleComp[key] + r[noteCol]
			else:
				alleleComp[key] = r[noteCol]
				strain[key] = r[strainCol]

		logger.debug ('Found %d genotypes' % len(strain))

		# extract assay note from query 1

		keyCol = Gatherer.columnNumber (self.results[1][0],
			'_Assay_key')
		noteCol = Gatherer.columnNumber (self.results[1][0],
			'assayNote')

		for r in self.results[1][1]:
			key = r[keyCol]
			if assayNote.has_key (key):
				assayNote[key] = assayNote[key] + r[noteCol]
			else:
				assayNote[key] = r[noteCol]

		logger.debug ('Found %d assay notes' % len(assayNote))

		# extract main data from final query

		cols = self.results[-1][0]
		alleleCol = Gatherer.columnNumber (cols, '_Allele_key')
		systemCol = Gatherer.columnNumber (cols, 'system')
		structureCol = Gatherer.columnNumber (cols, 'structure')
		assayTypeCol = Gatherer.columnNumber (cols, '_AssayType_key')
		reporterCol = Gatherer.columnNumber (cols,
			'_ReporterGene_key')
		assayCol = Gatherer.columnNumber (cols, '_Assay_key')
		ageCol = Gatherer.columnNumber (cols, 'age')
		sexCol = Gatherer.columnNumber (cols, 'sex')
		specimenNoteCol = Gatherer.columnNumber (cols, 'specimenNote')
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		resultNoteCol = Gatherer.columnNumber (cols, 'resultNote')
		strengthCol = Gatherer.columnNumber (cols, '_Strength_key')
		patternCol = Gatherer.columnNumber (cols, '_Pattern_key')
		jnumCol = Gatherer.columnNumber (cols, 'jnumID')

		self.finalResults = []
		self.finalColumns = [ '_Allele_key', 'system', 'structure',
			'age', 'sex', 'jnumID', 'resultNote', 'specimenNote',
			'level', 'pattern', 'figure', 'assayType',
			'reporterGene', 'detectionMethod', 
			'allelicComposition', 'strain', 'assayNote', ]

		for r in self.results[-1][1]:
			row = [ r[alleleCol], r[systemCol], r[structureCol],
				r[ageCol], r[sexCol], r[jnumCol],
				r[resultNoteCol], r[specimenNoteCol], ]
			row.append (Gatherer.resolve (r[strengthCol],
				'GXD_Strength', '_Strength_key', 'strength'))
			row.append (Gatherer.resolve (r[patternCol],
				'GXD_Pattern', '_Pattern_key', 'pattern'))
			row.append (None)
			row.append (Gatherer.resolve (r[assayTypeCol],
				'GXD_AssayType', '_AssayType_key',
				'assayType'))
			row.append (Gatherer.resolve (r[reporterCol]))
			row.append (None)

			genotype = r[genotypeCol]

			if alleleComp.has_key (genotype):
				row.append (alleleComp[genotype])
			else:
				row.append (None)

			if strain.has_key (genotype):
				row.append (strain[genotype])
			else:
				row.append (None)

			if assayNote.has_key (r[assayCol]):
				row.append (assayNote[r[assayCol]])
			else:
				row.append (None)

			self.finalResults.append (row)
		return

###--- globals ---###

cmds = [
	# genetic background info by genotype
	'''select distinct s._Genotype_key, mnc.note, mnc.sequenceNum,
		t.strain
	from all_cre_cache c,
		gxd_specimen s,
		mgi_note mn,
		mgi_notechunk mnc,
		gxd_genotype g,
		prb_strain t
	where c._Assay_key = s._Assay_key
		and s._Genotype_key = mn._Object_key
		and mn._NoteType_key = 1018
		and mn._Note_key = mnc._Note_key
		and s._Genotype_key = g._Genotype_key
		and g._Strain_key = t._Strain_key
	order by mnc.sequenceNum''',

	# assay notes by assay key
	'''select distinct a._Assay_key, a.sequenceNum, a.assayNote
	from gxd_assaynote a,
		all_cre_cache c
	where a._Assay_key = c._Assay_key
	order by a.sequenceNum''',

	# main cre assay result data
	'''select c._Allele_key, c.system, c.structure,
		a._AssayType_key, a._ReporterGene_key, a._Refs_key,
		a._Assay_key,
		s.age, s.sex, s.specimenNote, s._Genotype_key,
		r.resultNote, r._Strength_key, r._Pattern_key,
		b.jnumID
	from all_cre_cache c,
		gxd_assay a,
		gxd_specimen s,
		gxd_insituresult r,
		bib_citation_cache b
	where c._Assay_key = a._Assay_key
		and a._Assay_key = s._Assay_key
		and s._Specimen_key = r._Specimen_key
		and a._Refs_key = b._Refs_key''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Allele_key', 'system', 'structure', 'age', 'level',
	'pattern', 'jnumID', 'figure', 'assayType', 'reporterGene',
	'detectionMethod', 'sex', 'allelicComposition', 'strain',
	'assayNote', 'resultNote', 'specimenNote',
	]

# prefix for the filename of the output file
filenamePrefix = 'allele_recombinase_assay_result'

# global instance of a AlleleRecombinaseAssayResultGatherer
gatherer = AlleleRecombinaseAssayResultGatherer (filenamePrefix, fieldOrder,
	cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
