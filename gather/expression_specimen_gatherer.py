#!/usr/local/bin/python
# 
# gathers data for the 'assay_specimen' (and attached tables) in the front-end database
#
# HISTORY
#
# 02/07/2013    kstone
#	Initial add for TR11248 (Assay Detail revamp)
#

import Gatherer
import logger

###--- Classes ---###
NOT_SPECIFIED_VALUES = ['Not Specified','Not Applicable']
CONDITIONAL_GENOTYPE_NOTE = "Conditional mutant."

class SpecimenGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the expression_assay table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for assays,
	#	collates results, writes tab-delimited text file

	def collateResults(self):

		# process specimens +  results 
		specCols = [ 'specimen_key', 'assay_key', 'genotype_key','specimen_label','sex',
			'age','fixation','embedding_method','hybridization','age_note','specimen_note',
			'specimen_seq' ]
                specRows = []

		resultCols = [ 'specimen_result_key', 'specimen_key', 'structure',
			'structure_mgd_key','level','pattern','note','specimen_result_seq' ]
                resultRows = []

		imagepaneCols = [ 'specimen_result_imagepane_key','specimen_result_key', 'imagepane_key', 'imagepane_seq']
		imagepaneRows = []

		(cols, rows) = self.results[0]

		# define specimen columns from mgd query
		assayKeyCol = Gatherer.columnNumber (cols, '_assay_key')
		specKeyCol = Gatherer.columnNumber (cols, '_specimen_key')
		genotypeKeyCol = Gatherer.columnNumber (cols, '_genotype_key')
		specLabelCol = Gatherer.columnNumber (cols, 'specimenlabel')
		conditionalGenotypeCol = Gatherer.columnNumber (cols, 'conditional_genotype')
		sexCol = Gatherer.columnNumber (cols, 'sex')
		ageCol = Gatherer.columnNumber (cols, 'age')
		fixationCol = Gatherer.columnNumber (cols, 'fixation')
		embeddingCol = Gatherer.columnNumber (cols, 'embeddingmethod')
		hybridizationCol = Gatherer.columnNumber (cols, 'hybridization')
		ageNoteCol = Gatherer.columnNumber (cols, 'agenote')
		specimenNoteCol = Gatherer.columnNumber (cols, 'specimennote')
		specSeqCol = Gatherer.columnNumber (cols, 'specimen_seq')

		# define result columns from mgd query
		resultKeyCol = Gatherer.columnNumber (cols, '_result_key')
		structureCol = Gatherer.columnNumber (cols, 'printname')
		stageCol = Gatherer.columnNumber (cols, '_stage_key')
		structureKeyCol = Gatherer.columnNumber (cols, '_structure_key')
		strengthCol = Gatherer.columnNumber (cols, 'strength')
		patternCol = Gatherer.columnNumber (cols, 'pattern')
		resultNoteCol = Gatherer.columnNumber (cols, 'resultnote')
		resultSeqCol = Gatherer.columnNumber (cols, 'result_seq')
		imagepaneKeyCol = Gatherer.columnNumber (cols, '_imagepane_key')
	
		uniqueSpecimenKeys = set()
		uniqueResultKeys = {}
		resultCount = 0
		imagepaneCount = 0
		for row in rows:
			assayKey = row[assayKeyCol]
			specimenKey = row[specKeyCol]
			genotypeKey = row[genotypeKeyCol]
			specimenLabel = row[specLabelCol]
			isConditionalGenotype = row[conditionalGenotypeCol]
			sex = row[sexCol]
			age = row[ageCol]
			fixation = row[fixationCol]
			embedding = row[embeddingCol]
			hybridization = row[hybridizationCol]
			ageNote = row[ageNoteCol]
			specimenNote = row[specimenNoteCol]
			specimenSeq = row[specSeqCol]

			resultKey = row[resultKeyCol]
			structure = row[structureCol]
			structureMGDKey = row[structureKeyCol]
			stage = row[stageCol]
			# strength = detection level
			strength = row[strengthCol]
			pattern = row[patternCol]
			resultNote = row[resultNoteCol]
			resultSeq = row[resultSeqCol]

			imagepaneKey = row[imagepaneKeyCol]
	
			# hide not specified fixation method
			if fixation in NOT_SPECIFIED_VALUES:
				fixation = ""
			# hide note specified embedding method
			if embedding in NOT_SPECIFIED_VALUES:
				embedding = ""

			# hide not specified pattern
			if pattern in NOT_SPECIFIED_VALUES:
				pattern = ""
	
			# structure format is TS26: brain
			tsStructure = "TS%s: %s"%(stage,structure)

			# add conditional genotype note, if applicable
			if isConditionalGenotype == 1:
				specimenNote = specimenNote and "%s %s"%(CONDITIONAL_GENOTYPE_NOTE,specimenNote) or CONDITIONAL_GENOTYPE_NOTE

			if specimenKey not in uniqueSpecimenKeys:
				uniqueSpecimenKeys.add(specimenKey)
				# make a new specimen row
				specRows.append((specimenKey,assayKey,genotypeKey,specimenLabel,sex,
					age,fixation,embedding,hybridization,ageNote,specimenNote,specimenSeq))

			# we need to generate a unique result key, because result=>structure is not 1:1 relationship
			resultGenKey = (resultKey,structureMGDKey)
			if resultGenKey not in uniqueResultKeys:
				resultCount += 1
				uniqueResultKeys[resultGenKey] = resultCount
				# make a new specimen result row
				resultRows.append((resultCount,specimenKey,tsStructure,structureMGDKey,strength,pattern,resultNote,resultSeq))
			if imagepaneKey:
				imagepaneCount += 1
				# make a new imagepane row
				imagepaneRows.append((imagepaneCount,uniqueResultKeys[resultGenKey],imagepaneKey,imagepaneCount))
	
		#logger.debug("specimen rows = %s"%specRows)
		#logger.debug("result rows = %s"%resultRows)

		# Add all the column and row information to the output
		self.output = [(specCols,specRows),(resultCols,resultRows),(imagepaneCols,imagepaneRows)]
			
		return

	def postprocessResults(self):
		return

###--- globals ---###

cmds = [
	# 0. Gather all the specimens and their results 
	'''
	WITH imagepanes AS (
	select _result_key,_imagepane_key from gxd_insituresultimage
	UNION
	select _result_key,null from gxd_insituresult r1
		where not exists (select 1 from gxd_insituresultimage i1
			where r1._result_key=i1._result_key
		)
        )
	select gs._assay_key,gs._specimen_key,gs.specimenlabel,
	    gs.age,
	    gs.agenote,
	    gfm.fixation,
	    gs._genotype_key,
	    gg.isconditional conditional_genotype,
	    gs.sex,
	    gs.specimennote,
	    gem.embeddingmethod,
	    gs.hybridization,
	    gir._result_key,
	    struct.printname,struct._structure_key,struct._stage_key,str.strength,
	    gp.pattern, gir.resultnote,
	    gir.sequencenum as result_seq,
	    gs.sequencenum as specimen_seq,
	    giri._imagepane_key
	from gxd_specimen gs, gxd_insituresult gir, gxd_isresultstructure girs, 
	    gxd_structure struct,gxd_strength str, gxd_pattern gp, imagepanes giri,
	    gxd_fixationmethod gfm, gxd_embeddingmethod gem,gxd_genotype gg
	where gs._specimen_key=gir._specimen_key
	    and gir._result_key=girs._result_key
	    and girs._structure_key=struct._structure_key
	    and gir._strength_key=str._strength_key
	    and gir._pattern_key=gp._pattern_key
	    and gir._result_key=giri._result_key
	    and gfm._fixation_key=gs._fixation_key
	    and gem._embedding_key=gs._embedding_key
	    and gg._genotype_key=gs._genotype_key
	''',
	]

files = [
        ('assay_specimen',
		[ 'specimen_key', 'assay_key', 'genotype_key','specimen_label','sex',
			'age','fixation','embedding_method','hybridization','age_note','specimen_note',
			'specimen_seq' ],
                'assay_specimen'),

        ('specimen_result',
                [ 'specimen_result_key', 'specimen_key', 'structure',
                'structure_mgd_key','level','pattern','note','specimen_result_seq' ],
                'specimen_result'),

        ('specimen_result_to_imagepane',
                [ 'specimen_result_imagepane_key','specimen_result_key', 'imagepane_key', 'imagepane_seq'],
                'specimen_result_to_imagepane'),
        ]


# global instance of a SpecimenGatherer
gatherer = SpecimenGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
