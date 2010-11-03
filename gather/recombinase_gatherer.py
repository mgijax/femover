#!/usr/local/bin/python
# 
# gathers data for the recombinase tables in the front-end database.
#
# Because we are generating unique keys that only exist in the front-end
# database, and because we need those keys to drive relationships among the
# recombinase tables, we generate all those connected tables within this
# single script.  (This allows us to keep the keys in sync.)

import Gatherer
import logger
import symbolsort

###--- Functions ---###

def explode (x):
	# expand list x into a new list such that for each element i of x, we
	# have a tuple (i, all elements of x other than i)

	out = []
	for i in x:
		no_i = []
		for j in x:
			if i == j:
				continue
			no_i.append (j)
		out.append ( (i, no_i) )
	return out

###--- Classes ---###

class RecombinaseGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the recombinase tables
	# Has: queries to execute against the source database
	# Does: queries Sybase for primary data for recombinases,
	#	collates results, writes multiple tab-delimited text files

	def findAlleleSystemPairs (self):
		# Purpose: processes query 0 to identify allele/system pairs
		# Returns: alleleSystemMap, alleleData, columns, rows

		# alleleSystemMap[allele key] = {system : all/sys key}
		alleleSystemMap = {}

		# query 0 - defines allele / system pairs.  We need to number
		# these and store them in data set 0

		cols, rows = self.results[0]
		keyCol = Gatherer.columnNumber (cols, '_Allele_key')
		idCol = Gatherer.columnNumber (cols, 'accID')
		sysCol = Gatherer.columnNumber (cols, 'system')
		systemKeyCol = Gatherer.columnNumber (cols, '_System_key')
		symCol = Gatherer.columnNumber (cols, 'symbol')
		expCol = Gatherer.columnNumber (cols, 'expressed')

		alleleData = {}		# alleleData[key] = (ID, symbol)

		affectedCols = [ 'alleleKey', 'alleleSystemKey', 'system' ]
		unaffectedCols = [ 'alleleKey', 'alleleSystemKey', 'system' ]

		affected = []
		unaffected = []

		lastKey = None
		out = []
		i = 0
		for row in rows:

			# make sure to add the system for the allele key

			key = row[keyCol]
			system = row[sysCol]
			i = i + 1

			if alleleSystemMap.has_key(key):
				alleleSystemMap[key][system] = i
			else:
				alleleSystemMap[key] = { system : i }

			out.append ( (i, key, row[idCol], row[sysCol],
				row[systemKeyCol]) )

			# add this allele/system pair to either the list of
			# affected systems or unaffected systems

			triple = (key, i, system)
			if row[expCol] == 1:
				affected.append (triple)
			else:
				unaffected.append (triple)

			# if this is not the same as the last allele, then
			# we need to cache its ID and symbol

			if key == lastKey:
				continue
			alleleData[key] = (row[idCol], row[symCol])

			lastKey = key

		columns = [ 'alleleSystemKey', 'alleleKey', 'alleleID',
			'system', 'systemKey', ]

		logger.debug ('Found %d allele/system pairs' % i)
		logger.debug ('Found %d alleles' % len(alleleData))

		return alleleSystemMap, alleleData, columns, out, \
			affectedCols, affected, unaffectedCols, unaffected

	def findOtherSystems (self, alleleSystemMap):
		# Purpose: processes 'alleleSystemMap' to find other systems
		#	involved with the allele for each allele/system pair
		#	(does not touch query results in self.results)
		# Returns: columns, rows

		# we need to generate the data set for
		# the recombinase_other_system table (for each allele/system
		# pair, list the other systems associated with each allele)

		out = []
		i = 0
		alleles = alleleSystemMap.keys()
		for allele in alleles:
			systems = alleleSystemMap[allele].keys()
			systems.sort()

			for (system, otherSystems) in explode(systems):
				alleleSystemKey = \
					alleleSystemMap[allele][system]
				for otherSystem in otherSystems:
					i = i + 1
					out.append ( (i, alleleSystemKey,
						otherSystem) )

		columns = [ 'uniqueKey', 'alleleSystemKey', 'system' ]

		logger.debug ('Found %d other systems' % i)

		return columns, out

	def findOtherAlleles (self, alleleSystemMap, alleleData):
		# Purpose: processes 'alleleSystemMap' to find other alleles
		#	involved with the system for each allele/system pair
		#	(does not touch query results in self.results)
		# Returns: columns, rows

		# we will generate the data set for the
		# recombinase_other_allele table (for each allele/system pair,
		# list the other alleles associated with each system)

		bySystem = {}		# bySystem[system] = list of alleles

		alleles = alleleSystemMap.keys()
		for allele in alleles:
			systems = alleleSystemMap[allele].keys()
			for system in systems:
				if not bySystem.has_key (system):
					bySystem[system] = [ allele ]
				else:
					bySystem[system].append (allele)

		out = []
		i = 0

		for system in bySystem.keys():
			alleles = bySystem[system]
			for (allele, otherAlleles) in explode(alleles):
				alleleSystemKey = \
					alleleSystemMap[allele][system]

				for other in otherAlleles:	
					i = i + 1
					out.append ( (
						i,
						alleleSystemKey,
						other,
						alleleData[other][0],
						alleleData[other][1],
						) )

		columns = [ 'uniqueKey', 'alleleSystemKey', 'alleleKey',
				'alleleID', 'alleleSymbol', ]

		logger.debug ('Found %d other alleles' % i)

		return columns, out

	def findGenotypes (self):
		# extract allelic composition and background strain from
		# query 1

		keyCol = Gatherer.columnNumber (self.results[1][0],
			'_Genotype_key')
		noteCol = Gatherer.columnNumber (self.results[1][0], 'note')
		strainCol = Gatherer.columnNumber (self.results[1][0],
			'strain')

		alleleComp = {}
		strain = {}

		for r in self.results[1][1]:
			key = r[keyCol]
			if alleleComp.has_key (key):
				alleleComp[key] = alleleComp[key] + r[noteCol]
			else:
				alleleComp[key] = r[noteCol]
				strain[key] = r[strainCol]

		# trim trailing whitespace from allelic composition notes

		for key in alleleComp.keys():
			alleleComp[key] = alleleComp[key].rstrip()

		logger.debug ('Found %d genotypes' % len(strain))
		return alleleComp, strain

	def findAssayNotes (self):
		# extract assay note from query 2

		keyCol = Gatherer.columnNumber (self.results[2][0],
			'_Assay_key')
		noteCol = Gatherer.columnNumber (self.results[2][0],
			'assayNote')

		assayNote = {}

		for r in self.results[2][1]:
			key = r[keyCol]
			if assayNote.has_key (key):
				assayNote[key] = assayNote[key] + r[noteCol]
			else:
				assayNote[key] = r[noteCol]

		logger.debug ('Found %d assay notes' % len(assayNote))
		return assayNote

	def findNamesIDs (self, results):
		ids = {}
		names = {}

		columns, rows = results
		prepCol = Gatherer.columnNumber (columns, '_ProbePrep_key')
		nameCol = Gatherer.columnNumber (columns, 'name')
		idCol = Gatherer.columnNumber (columns, 'accID')
		
		for r in rows:
			key = r[prepCol]

			# only keep the first ID and name found for
			# each probe/antibody

			if not ids.has_key(key):
				ids[key] = r[idCol]
				names[key] = r[nameCol] 
		return ids, names

	def findAssayResults (self, alleleSystemMap):

		# preliminary extractions from queries 1-4

		alleleComp, strain = self.findGenotypes()
		assayNotes = self.findAssayNotes()
		probeIDs, probeNames = self.findNamesIDs (self.results[3])
		antibodyIDs, antibodyNames = self.findNamesIDs (
			self.results[4])

		# detailed assay results from query 5

		cols = self.results[5][0]

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
		pPrepCol = Gatherer.columnNumber (cols, '_ProbePrep_key')
		aPrepCol = Gatherer.columnNumber (cols, '_AntibodyPrep_key')
		dbResultCol = Gatherer.columnNumber (cols, '_Result_key')

		out = []
		columns = [ 'resultKey', 'alleleSystemKey', 'structure',
			'age', 'sex', 'jnumID', 'resultNote', 'specimenNote',
			'level', 'pattern', 'assayType', 'reporterGene',
			'detectionMethod', 'allelicComposition', 'strain',
			'assayNote', 'probeID', 'probeName', 'antibodyID',
			'antibodyName',
			]

		# newResultKeys[db result key] = [ (new result key, new
		#	allele/system key), ... ]
		newResultKeys = {}

		i = 0
		for r in self.results[5][1]:
			alleleSystemKey = \
				alleleSystemMap[r[alleleCol]][r[systemCol]]

			i = i + 1

			# map the old result key (from the database) to the
			# new result key(s) and the allele/system keys being
			# generated

			info = (i, alleleSystemKey)
			dbResultKey = r[dbResultCol]
			if newResultKeys.has_key(dbResultKey):
				newResultKeys[dbResultKey].append (info)
			else:
				newResultKeys[dbResultKey] = [ info ]

			row = [ i, alleleSystemKey, r[structureCol],
				r[ageCol], r[sexCol], r[jnumCol],
				r[resultNoteCol], r[specimenNoteCol], ]
			row.append (Gatherer.resolve (r[strengthCol],
				'GXD_Strength', '_Strength_key', 'strength'))
			row.append (Gatherer.resolve (r[patternCol],
				'GXD_Pattern', '_Pattern_key', 'pattern'))
			row.append (Gatherer.resolve (r[assayTypeCol],
				'GXD_AssayType', '_AssayType_key',
				'assayType'))
			row.append (Gatherer.resolve (r[reporterCol]))

			pPrep = r[pPrepCol]
			aPrep = r[aPrepCol]

			if probeIDs.has_key(pPrep):
				row.append ('probe')
			elif antibodyIDs.has_key(aPrep):
				row.append ('antibody')
			else:
				row.append ('direct')

			genotype = r[genotypeCol]

			if alleleComp.has_key (genotype):
				row.append (alleleComp[genotype])
			else:
				row.append (None)

			if strain.has_key (genotype):
				row.append (strain[genotype])
			else:
				row.append (None)

			if assayNotes.has_key (r[assayCol]):
				row.append (assayNotes[r[assayCol]])
			else:
				row.append (None)

			toDo = [ (pPrep, (probeIDs, probeNames) ), 
				(aPrep, (antibodyIDs, antibodyNames) ) ]

			for (prep, dicts) in toDo:
				for dict in dicts:
					if dict.has_key(prep):
						row.append (dict[prep])
					else:
						row.append (None)

			out.append (row)

		logger.debug ('Found %d assay results' % len(out))
		logger.debug ('Mapped %d assay results' % len(newResultKeys))
		return columns, out, newResultKeys

	def findSortValues (self, cols, rows):
		keyCol = Gatherer.columnNumber (cols, 'resultKey')

		byFields = [ 'structure', 'age', 'level', 'pattern', 'jnumID',
			'assayType', 'reporterGene', 'detectionMethod',
			'assayNote', 'allelicComposition', 'sex',
			'specimenNote', 'resultNote' ]

		sorted = {}		# sorted[key] = [ field1, ... ]
		for row in rows:
			sorted[row[keyCol]] = [ row[keyCol] ]

		for field in byFields:
			nullValued = []
			sortable = []

			myCol = Gatherer.columnNumber (cols, field)
			for row in rows:
				myVal = row[myCol]
				if myVal == None:
					nullValued.append (row[keyCol])
				else:
					sortable.append ((myVal.lower(),
						row[keyCol]))
			sortable.sort()

			i = 1
			for (myValue, key) in sortable:
				sorted[key].append (i)
				i = i + 1

			for key in nullValued:
				sorted[key].append (i)

			logger.debug ('Sorted %d rows by %s' % (i - 1, field))

		allRows = sorted.values()
		allRows.sort()
		logger.debug ('Sorted output rows prior to load')

		return [ 'resultKey' ] + byFields, allRows

	def findResultImages (self,
		resultMap	# maps from old result key to (new result key,
				# new allele/system key)
		):

		# associations between results and image panes is in query 6

		columns, rows = self.results[6]

		resultCol = Gatherer.columnNumber (columns, '_Result_key')
		labelCol = Gatherer.columnNumber (columns, 'paneLabel')
		imageCol = Gatherer.columnNumber (columns, '_Image_key')

		# columns for assay result image panes
		arColumns = [ 'uniqueKey', 'resultKey', 'imageKey',
			'sequenceNum', 'paneLabel', ]

		# columns for allele system images
		asColumns = [ 'uniqueKey', 'alleleSystemKey', 'imageKey',
			'sequenceNum', ]

		arRows = []	# rows for assay result image panes
		asRows = []	# rows for allele system image panes

		arNum = 0	# sequence number for assay result image panes
		asNum = 0	# sequence number for allele system img panes

		done = {}	# done[(allele/system key, image key)] = 1

		lastResultKey = None
		lastSystemKey = None

		arMax = 0
		asMax = 0

		for row in rows:
		    if not resultMap.has_key(row[resultCol]):
			logger.debug ('Unknown result key: %s' % \
				row[resultCol])
			continue
		    for (resultKey, alleleSysKey) in resultMap[row[resultCol]]:
			imageKey = row[imageCol]
			label = row[labelCol]

			if resultKey == lastResultKey:
				arNum = arNum + 1
			else:
				arNum = 1
				lastResultKey = resultKey

			arMax = arMax + 1
			arRows.append ( (arMax, resultKey, imageKey, arNum,
				label) )

			pair = (alleleSysKey, imageKey)
			if not done.has_key (pair):

				if alleleSysKey != lastSystemKey:
					asNum = asNum + 1
				else:
					asNum = 1
					lastSystemKey = alleleSysKey

				asMax = asMax + 1
				asRows.append ( (asMax, alleleSysKey,
					imageKey, asNum) )
				done[pair] = 1

		return arColumns, arRows, asColumns, asRows

	def collateResults (self):

		# step 1 -- recombinase_allele_system table

		alleleSystemMap, alleleData, columns, rows, \
		affectedCols, affected, unaffectedCols, unaffected = \
			self.findAlleleSystemPairs()
		self.output.append ( (columns, rows) )

		# step 2 -- recombinase_other_system table

		columns, rows = self.findOtherSystems (alleleSystemMap)
		self.output.append ( (columns, rows) )

		# step 3 -- recombinase_other_allele table

		columns, rows = self.findOtherAlleles (alleleSystemMap,
			alleleData)
		self.output.append ( (columns, rows) )

		# step 4 -- recombinase_assay_result table

		columns, rows, resultMap = \
			self.findAssayResults (alleleSystemMap)
		self.output.append ( (columns, rows) )

		# step 5 -- recombinase_assay_result_sequence_num table

		columns, rows = self.findSortValues (columns, rows)
		self.output.append ( (columns, rows) )

		# step 6 - recombinase_assay_result_imagepane and
		#	recombinase_allele_system_imagepane tables
		arColumns, arRows, asColumns, asRows = self.findResultImages (
			resultMap)
		self.output.append ( (asColumns, asRows) )
		self.output.append ( (arColumns, arRows) )

		# step 7 - allele_recombinase_affected_system and
		#	allele_recombinase_unaffected_system tables

		self.output.append ( (affectedCols, affected) )
		self.output.append ( (unaffectedCols, unaffected) )
		return

###--- globals ---###

# SQL commands to be executed against the source database
cmds = [
	# all allele / system pairs; order by logical db to prioritize MGI IDs
	'''select distinct c._Allele_key, a.accID, c.system, a._LogicalDB_key,
		c.symbol, c.expressed, c._System_key
	from all_cre_cache c,
		acc_accession a
	where c._Allele_key = a._Object_key
		and a.preferred = 1
		and a.private = 0
		and a._MGIType_key = 11
	order by c._Allele_key, a._LogicalDB_key, c.system''',

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

	# probes for each Cre probe prep key

	'''select g._ProbePrep_key,
		p.name,
		acc.accID,
		acc._LogicalDB_key
	from all_cre_cache c,
		gxd_assay a,
		gxd_probeprep g,
		prb_probe p,
		acc_accession acc
	where c._Assay_key = a._Assay_key
		and a._ProbePrep_key = g._ProbePrep_key
		and g._Probe_key = p._Probe_key
		and g._Probe_key = acc._Object_key
		and acc._MGIType_key = 3
		and acc.preferred = 1
		and acc.private = 0
	order by acc._LogicalDB_key''',		# MGI IDs preferred

	# antibodies for each Cre antibody key

	'''select g._AntibodyPrep_key as _ProbePrep_key,
		p.antibodyName as name,
		acc.accID,
		acc._LogicalDB_key
	from all_cre_cache c,
		gxd_assay a,
		gxd_antibodyprep g,
		gxd_antibody p,
		acc_accession acc
	where c._Assay_key = a._Assay_key
		and a._AntibodyPrep_key = g._AntibodyPrep_key
		and g._Antibody_key = p._Antibody_key
		and g._Antibody_key = acc._Object_key
		and acc._MGIType_key = 6
		and acc.preferred = 1
		and acc.private = 0
	order by acc._LogicalDB_key''',		# MGI IDs preferred

	# main cre assay result data
	'''select distinct c._Allele_key, c.system, c.structure,
		a._AssayType_key, a._ReporterGene_key, a._Refs_key,
		a._Assay_key, a._ProbePrep_key, a._AntibodyPrep_key,
		s.age, s.sex, s.specimenNote, s._Genotype_key,
		r.resultNote, r._Strength_key, r._Pattern_key, r._Result_key,
		b.jnumID
	from all_cre_cache c,
		gxd_assay a,
		gxd_specimen s,
		gxd_insituresult r,
		gxd_isresultstructure rs,
		bib_citation_cache b
	where c._Assay_key = a._Assay_key
		and a._Assay_key = s._Assay_key
		and s._Specimen_key = r._Specimen_key
		and c._Structure_key = rs._Structure_key
		and rs._Result_key = r._Result_key
		and a._Refs_key = b._Refs_key''',

	# image panes associated with recombinase assay results
	'''select distinct g._Result_key, i.paneLabel, i._Image_key
	from img_imagepane i,
		gxd_insituresultimage g,
		gxd_insituresult r,
		gxd_specimen s,
		all_cre_cache c
	where g._ImagePane_key = i._ImagePane_key
		and g._Result_key = r._Result_key
		and r._Specimen_key = s._Specimen_key
		and s._Assay_key = c._Assay_key''',
	]

# data about files to be written; for each:  (filename prefix, list of field
#	names in order to be written, name of table to be loaded)
files = [
	('recombinase_allele_system',
		[ 'alleleSystemKey', 'alleleKey', 'alleleID', 'system',
			'systemKey', ],
		'recombinase_allele_system'),

	('recombinase_other_system',
		[ 'uniqueKey', 'alleleSystemKey', 'system' ],
		'recombinase_other_system'),

	('recombinase_other_allele',
		[ 'uniqueKey', 'alleleSystemKey', 'alleleKey', 'alleleID',
			'alleleSymbol', ],
		'recombinase_other_allele'),

	('recombinase_assay_result',
		[ 'resultKey', 'alleleSystemKey', 'structure', 'age', 'level',
			'pattern', 'jnumID', 'assayType', 'reporterGene',
			'detectionMethod', 'sex', 'allelicComposition',
			'strain', 'assayNote', 'resultNote',
			'specimenNote', 'probeID', 'probeName', 'antibodyID',
			'antibodyName', ],
		'recombinase_assay_result'),

	('recombinase_assay_result_sequence_num',
		[ 'resultKey', 'structure', 'age', 'level', 'pattern',
			'jnumID', 'assayType', 'reporterGene',
			'detectionMethod', 'assayNote', 'allelicComposition',
			'sex', 'specimenNote', 'resultNote' ],
		'recombinase_assay_result_sequence_num'),

	('recombinase_allele_system_imagepane',
		[ 'uniqueKey', 'alleleSystemKey', 'imageKey', 'sequenceNum' ],
		'recombinase_allele_system_to_image'),

	('recombinase_assay_result_imagepane',
		[ 'uniqueKey', 'resultKey', 'imageKey', 'sequenceNum',
			'paneLabel', ],
		'recombinase_assay_result_imagepane'),

	('allele_recombinase_affected_system',
		[ Gatherer.AUTO, 'alleleKey', 'alleleSystemKey', 'system' ],
		'allele_recombinase_affected_system'),

	('allele_recombinase_unaffected_system',
		[ Gatherer.AUTO, 'alleleKey', 'alleleSystemKey', 'system' ],
		'allele_recombinase_unaffected_system'),
	]

# global instance of a RecombinaseGatherer
gatherer = RecombinaseGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
