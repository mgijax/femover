#!/usr/local/bin/python
# 
# gathers data for the 'expression_result_summary' and 
# 'expression_result_to_imagepane' tables in the front-end database

import Gatherer
import logger
import ADVocab

###--- Functions ---###

def getIsExpressed(strength):
	# convert the full 'strength' value (detection level) to its
	# counterpart for the 3-valued 'is expressed?' flag

	s = strength.lower()
	if s == 'absent':
		return 'No'
	elif s in [ 'ambiguous', 'not specified' ]:
		return 'Unknown/Ambiguous'
	return 'Yes'

###--- Classes ---###

class ExpressionResultSummaryGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the expression_result_summary and
	#	expression_result_to_imagepane tables
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for expression
	#	results and imagepanes, collates data, writes tab-delimited
	#	text files

	def getAssayIDs(self):
		# handle query 0 : returns { assay key : MGI ID }
		ids = {}

		cols, rows = self.results[0]

		idCol = Gatherer.columnNumber (cols, 'accID')
		keyCol = Gatherer.columnNumber (cols, '_Assay_key')

		for row in rows:
			ids[row[keyCol]] = row[idCol]

		logger.debug ('Got %d assay IDs' % len(ids))
		return ids

	def getJnumbers(self):
		# handle query 1 : returns { refs key : jnum ID }
		jnum = {}

		cols, rows = self.results[1]

		idCol = Gatherer.columnNumber (cols, 'accID')
		keyCol = Gatherer.columnNumber (cols, '_Refs_key')

		for row in rows:
			jnum[row[keyCol]] = row[idCol]

		logger.debug ('Got %d assay IDs' % len(jnum))
		return jnum

	def getMarkerSymbols(self):
		# handle query 2 : returns { marker key : marker symbol }
		symbol = {}

		cols, rows = self.results[2]

		symbolCol = Gatherer.columnNumber (cols, 'symbol')
		keyCol = Gatherer.columnNumber (cols, '_Marker_key')

		for row in rows:
			symbol[row[keyCol]] = row[symbolCol]

		logger.debug ('Got %d marker symbols' % len(symbol))
		return symbol

	def getDisplayableImagePanes(self):
		# handle query 3 : returns { image pane key : 1 }
		panes = {}

		cols, rows = self.results[3]

		for row in rows:
			panes[row[0]] = 1

		logger.debug ('Got %d displayable image panes' % len(panes))
		return panes

	def getRecombinaseReporterGeneKeys(self):
		# handle query 4 : returns { recomb. reporter gene key : 1 }
		genes = {}

		cols, rows = self.results[4]

		for row in rows:
			genes[row[0]] = 1

		logger.debug('Got %d recombinase reporter genes' % len(genes))
		return genes

	def getPanesForInSituResults(self):
		# handle query 5 : returns { assay key : [ image pane keys ] }
		panes = {}

		cols, rows = self.results[5]

		assayCol = Gatherer.columnNumber (cols, '_Result_key')
		paneCol = Gatherer.columnNumber (cols, '_ImagePane_key')

		for row in rows:
			assay = row[assayCol]
			if panes.has_key(assay):
				panes[assay].append (row[paneCol])
			else:
				panes[assay] = [ row[paneCol] ]

		logger.debug ('Got panes for %d in situ assays' % len(panes))
		return panes

	def getBasicAssayData(self):
		# handle query 6 : returns [ [ assay key, assay type,
		#	reference key, marker key, image pane key,
		#	reporter gene key, is gel flag ], ... ]
		assays = []

		cols, rows = self.results[6]

		assayCol = Gatherer.columnNumber (cols, '_Assay_key')
		assayTypeCol = Gatherer.columnNumber (cols, 'assayType')
		paneCol = Gatherer.columnNumber (cols, '_ImagePane_key')
		refsCol = Gatherer.columnNumber (cols, '_Refs_key')
		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		reporterCol = Gatherer.columnNumber (cols,'_ReporterGene_key')
		isGelCol = Gatherer.columnNumber (cols, 'isGelAssay')

		for row in rows:
			assays.append ( [ row[assayCol], row[assayTypeCol],
				row[refsCol], row[markerCol], row[paneCol],
				row[reporterCol], row[isGelCol] ] )

		logger.debug ('Got basic data for %d assays' % len(assays))
		return assays

	def getInSituExtraData(self):
		# handle query 7 : returns { assay key : [ genotype key,
		#	age, strength, result key, structure key ] }
		extras = {}

		cols, rows = self.results[7]

		assayCol = Gatherer.columnNumber (cols, '_Assay_key')
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		ageCol = Gatherer.columnNumber (cols, 'age')
		strengthCol = Gatherer.columnNumber (cols, 'strength')
		resultCol = Gatherer.columnNumber (cols, '_Result_key')
		structureCol = Gatherer.columnNumber (cols,'_Structure_key')

		for row in rows:
		    assayKey = row[assayCol]
		    if extras.has_key(assayKey):
	 		extras[assayKey].append( [ row[genotypeCol],
				row[ageCol], row[strengthCol], row[resultCol],
				row[structureCol] ] )
		    else:
	 		extras[assayKey] = [ [ row[genotypeCol],
				row[ageCol], row[strengthCol], row[resultCol],
				row[structureCol] ] ]


		logger.debug ('Got extra data for %d in situ assays' % \
			len(extras))
		return extras

	def getGelExtraData(self):
		# handle query 8 : returns { assay key : [ genotype key,
		#	age, strength, structure key ] }
		extras = {}

		cols, rows = self.results[8]

		assayCol = Gatherer.columnNumber (cols, '_Assay_key')
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		ageCol = Gatherer.columnNumber (cols, 'age')
		strengthCol = Gatherer.columnNumber (cols, 'strength')
		structureCol = Gatherer.columnNumber (cols,'_Structure_key')

		for row in rows:
		    assayKey = row[assayCol]
		    if extras.has_key(assayKey):
			extras[row[assayCol]].append( [ row[genotypeCol],
				row[ageCol], row[strengthCol],
				row[structureCol] ] )
		    else:
			extras[row[assayCol]] = [ [ row[genotypeCol],
				row[ageCol], row[strengthCol],
				row[structureCol] ] ]

		logger.debug ('Got extra data for %d gel assays' % \
			len(extras))
		return extras

	def collateResults (self):

		# get caches of data

		assayIDs = self.getAssayIDs()
		jNumbers = self.getJnumbers()
		symbols = self.getMarkerSymbols()
		displayablePanes = self.getDisplayableImagePanes()
		recombinaseReporters = self.getRecombinaseReporterGeneKeys()
		panesForInSituResults = self.getPanesForInSituResults()

		assays = self.getBasicAssayData()
		inSituExtras = self.getInSituExtraData()
		gelExtras = self.getGelExtraData()

		# definitions for expression_result_summary table data

		ersCols = [ 'result_key', '_Assay_key', 'assayType',
			'assayID', '_Marker_key', 'symbol', 'system', 'stage',
			'age', 'structure', 'printname', 'structureKey',
			'detectionLevel', 'isExpressed', '_Refs_key',
			'jnumID', 'hasImage', '_Genotype_key' ]
		ersRows = []

		# definitions for expression_result_to_imagepane table data

		ertiCols = [ 'result_key', '_ImagePane_key',
			'sequence_num' ]
		ertiRows = []

		# now, we need to walk through our assays to populate the list
		# of data for each table

		newKey = 0		# unique key for each result

		for [ assayKey, assayType, refsKey, markerKey, imagepaneKey,
		    reporterGeneKey, isGel ] in assays:
			
		    if isGel:
			extras = gelExtras[assayKey]
		    else:
			extras = inSituExtras[assayKey]

		    for items in extras:
			hasImage = 0	# is there a displayable image for
					# ...this result?

			panes = []	# image panes for this result

			if isGel:
			    [ genotypeKey, age, strength, structureKey ] = \
				items

			    if imagepaneKey:
				panes = [ imagepaneKey ]

				if displayablePanes.has_key(imagepaneKey):
				    hasImage = 1
			else:
			    [ genotypeKey, age, strength, resultKey,
				structureKey ] = items

			    if panesForInSituResults.has_key(resultKey):
				panes = panesForInSituResults[resultKey]

				for paneKey in panes:
				    if displayablePanes.has_key(paneKey):
					hasImage = 1
					break

			newKey = newKey + 1

			outRow = [ newKey,
			    assayKey,
			    assayType,
			    assayIDs[assayKey],
			    markerKey,
			    symbols[markerKey],
			    ADVocab.getSystem(structureKey),
			    ADVocab.getStage(structureKey),
			    age,
			    ADVocab.getStructure(structureKey),
			    ADVocab.getPrintname(structureKey),
			    ADVocab.getTermKey(structureKey),
			    strength,
			    getIsExpressed(strength),
			    refsKey,
			    jNumbers[refsKey],
			    hasImage,
			    genotypeKey,
			    ]

			ersRows.append (outRow)

			for paneKey in panes:
			    paneRow = [ newKey, paneKey, len(ertiRows) + 1 ]
			    ertiRows.append (paneRow)

#		    hasImage = 0	# is there a displayable image for this
#		    			# ...result?
#		    panes = []		# panes to associate with this result
#
#		    if isGel:
#			[ genotypeKey, age, strength, structureKey ] = \
#				gelExtras[assayKey]
#
#			if displayablePanes.has_key(imagepaneKey):
#				hasImage = 1
#
#			if imagepaneKey:
#				panes = [ imagepaneKey ]
#
#		    else:
#			[ genotypeKey, age, strength, resultKey,
#				structureKey ] = inSituExtras[assayKey]
#
#			if panesForInSituResults.has_key(resultKey):
#			    panes = panesForInSituResults[resultKey]
#
#			    for paneKey in panes:
#				if displayablePanes.has_key(paneKey):
#				    hasImage = 1

		logger.debug ('Got %d GXD result summary rows' % len(ersRows))
		logger.debug ('Got %d GXD result/pane rows' % len(ertiRows))

		self.output.append ( (ersCols, ersRows) )
		self.output.append ( (ertiCols, ertiRows) )
		return

###--- globals ---###

cmds = [
	# While it would be nice to make use of the GXD_Expression table to
	# assemble our data, it does not provide access to some information we
	# need, like the raw 'strength' of detection values.  So, we are left
	# building the summary data back up from the base tables.

	# 0. MGI IDs for expression assays
	'''select _Object_key as _Assay_key,
		accID
	from ACC_Accession
	where _MGIType_key = 8
		and _LogicalDB_key = 1
		and private = 0
		and preferred = 1''',

	# 1. J: numbers (IDs) for references with expression data
	'''select a._Object_key as _Refs_key,
		a.accID
	from ACC_Accession a
	where a._MGIType_key = 1
		and a.private = 0
		and a.preferred = 1
		and a._LogicalDB_key = 1
		and a.prefixPart = 'J:'
		and exists (select 1 from GXD_Assay g
			where a._Object_key = g._Refs_key)''',

	# 2. marker symbols studied in expression data
	'''select m._Marker_key, m.symbol
	from MRK_Marker m
	where exists (select 1 from GXD_Assay g
		where m._Marker_key = g._Marker_key)''',

	# 3. list of image panes which have dimensions (indicates whether we
	# have the actual image available for display or not)

	'''select p._ImagePane_key
	from IMG_ImagePane p, IMG_Image i, VOC_Term t
	where p._Image_key = i._Image_key
		and i.xDim is not null
		and i._ImageClass_key = t._Term_key
		and t.term = 'Expression' ''',

	# 4. set of reporter genes which would indicate a recombinase assay
	# (for assay types where it's not automatically a recombinase assay)
	'''select msm._Object_key as _ReporterGene_key
	from MGI_Set ms, MGI_SetMember msm
	where ms._Set_key = msm._Set_key
		and ms.name = 'Recombinases' ''',

	# 5. image panes for in situ results
	'''select distinct i._Result_key, i._ImagePane_key
	from GXD_InSituResultImage i''',

	# 6. basic assay data (image key here is for gel assays)
	'''select a._Assay_key, a._AssayType_key, t.assayType,
		a._Refs_key, a._Marker_key, a._ImagePane_key,
		a._ReporterGene_key, t.isGelAssay
	from GXD_Assay a,
		GXD_AssayType t
	where a._AssayType_key = t._AssayType_key''',

	# 7. additional data for in situ assays (note that there can be > 1
	# structures per result key)

	'''select distinct s._Assay_key,
		s._Genotype_key,
		s.age,
		r._Strength_key,
		st.strength,
		r._Result_key,
		rs._Structure_key
	from GXD_Specimen s,
		GXD_InSituResult r,
		GXD_Strength st,
		GXD_ISResultStructure rs
	where s._Specimen_key = r._Specimen_key
		and r._Strength_key = st._Strength_key
		and r._Result_key = rs._Result_key''', 

	# 8. additional data for gel assays (skip control lanes)  (note that
	# there can be > 1 structures per gel lane)

	'''select distinct g._Assay_key,
		g._Genotype_key,
		g.age,
		st._Strength_key,
		st.strength,
		gs._Structure_key
	from GXD_GelLane g,
		GXD_GelBand b,
		GXD_Strength st,
		GXD_GelLaneStructure gs
	where g._GelControl_key = 1
		and g._GelLane_key = b._GelLane_key
		and b._Strength_key = st._Strength_key
		and g._GelLane_key = gs._GelLane_key''',
	]

files = [
	('expression_result_summary',
		[ 'result_key', '_Assay_key', 'assayType', 'assayID',
		'_Marker_key', 'symbol', 'system', 'stage', 'age',
		'structure', 'printname', 'structureKey', 'detectionLevel',
		'isExpressed', '_Refs_key', 'jnumID', 'hasImage',
		'_Genotype_key' ],
		'expression_result_summary'),

	('expression_result_to_imagepane',
		[ Gatherer.AUTO, 'result_key', '_ImagePane_key',
		'sequence_num' ],
		'expression_result_to_imagepane'),
	]

# global instance of a ExpressionResultSummaryGatherer
gatherer = ExpressionResultSummaryGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
