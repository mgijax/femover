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

###--- Globals ---###

uniqueSpecimenKeys = set()
uniqueResultKeys = {}
resultCount = 0
imagepaneCount = 0

###--- Classes ---###
NOT_SPECIFIED_VALUES = ['Not Specified','Not Applicable']
CONDITIONAL_GENOTYPE_NOTE = "Conditional mutant."

class SpecimenGatherer (Gatherer.CachingMultiFileGatherer):
	# Is: a data gatherer for the expression_assay table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for assays,
	#	collates results, writes tab-delimited text file

	def collateResults(self):
		global uniqueSpecimenKeys, uniqueResultKeys
		global resultCount, imagepaneCount

		# process specimens +  results 

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
		stageCol = Gatherer.columnNumber (cols, '_stage_key')
		emapsKeyCol = Gatherer.columnNumber (cols, '_emaps_key')
		structureCol = Gatherer.columnNumber (cols, 'structure')
		strengthCol = Gatherer.columnNumber (cols, 'strength')
		patternCol = Gatherer.columnNumber (cols, 'pattern')
		resultNoteCol = Gatherer.columnNumber (cols, 'resultnote')
		resultSeqCol = Gatherer.columnNumber (cols, 'result_seq')
		imagepaneKeyCol = Gatherer.columnNumber (cols, '_imagepane_key')
		
		
		# group by specimen
		specimenGroups = {}
		for row in rows:
			specimenGroups.setdefault(row[specKeyCol], []).append(row)
		
		
		for specimenKey, group in specimenGroups.items():
			
			
			# specimen result + structure sequence num
			seqnum = 0
			
			# sort rows by resultSeq + printname
			group.sort(key = lambda x: (x[resultSeqCol], x[stageCol], x[structureCol]) )
	
			for row in group:
				
				seqnum += 1
				assayKey = row[assayKeyCol]
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
	
				#structure = row[structureCol]
				#stage = row[stageCol]
	
				emapsKey = row[emapsKeyCol]
				structure = row[structureCol]
				stage = row[stageCol]
	
				if (not emapsKey) or (not stage) or (not structure):
					continue
	
				# strength = detection level
				strength = row[strengthCol]
				pattern = row[patternCol]
				resultNote = row[resultNoteCol]
				resultSeq = row[resultSeqCol]
	
				imagepaneKey = row[imagepaneKeyCol]
		
				# assign default text to null specimenLabels
				specimenLabel = not specimenLabel and "%s"%specimenSeq or specimenLabel
	
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
				tsStructure = "TS%s: %s"%(int(stage),structure)
	
				# add conditional genotype note, if applicable
				if isConditionalGenotype == 1:
					specimenNote = specimenNote and "%s %s"%(CONDITIONAL_GENOTYPE_NOTE,specimenNote) or CONDITIONAL_GENOTYPE_NOTE
	
				if specimenKey not in uniqueSpecimenKeys:
					uniqueSpecimenKeys.add(specimenKey)
					# make a new specimen row
					self.addRow('assay_specimen', (specimenKey,assayKey,genotypeKey,specimenLabel,sex,
						age,fixation,embedding,hybridization,ageNote,specimenNote,specimenSeq))
	
				# we need to generate a unique result key, because result=>structure is not 1:1 relationship
				resultGenKey = (resultKey,emapsKey)
				if resultGenKey not in uniqueResultKeys:
					resultCount += 1
					uniqueResultKeys[resultGenKey] = resultCount
					# make a new specimen result row
					self.addRow('specimen_result', (resultCount,specimenKey,tsStructure,emapsKey,strength,pattern,resultNote,seqnum))
				if imagepaneKey:
					imagepaneCount += 1
					# make a new imagepane row
					self.addRow('specimen_result_to_imagepane', (imagepaneCount,uniqueResultKeys[resultGenKey],imagepaneKey,imagepaneCount))
	
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
	select gs._assay_key,
		gs._specimen_key,
		gs.specimenlabel,
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
	    str.strength,
	    gp.pattern, gir.resultnote,
	    gir.sequencenum as result_seq,
	    gs.sequencenum as specimen_seq,
	    giri._imagepane_key,
	    girs._stage_key,
	    vte._term_key as _emaps_key,
	    struct.term as structure
	from gxd_specimen gs, gxd_insituresult gir, gxd_isresultstructure girs, 
	    gxd_strength str, gxd_pattern gp, imagepanes giri,
	    gxd_fixationmethod gfm, gxd_embeddingmethod gem,gxd_genotype gg,
	    voc_term_emaps vte, voc_term struct
	where gs._specimen_key=gir._specimen_key
	    and gir._result_key=girs._result_key
	    and gir._strength_key=str._strength_key
	    and gir._pattern_key=gp._pattern_key
	    and gir._result_key=giri._result_key
	    and gfm._fixation_key=gs._fixation_key
	    and gem._embedding_key=gs._embedding_key
	    and gg._genotype_key=gs._genotype_key
	    and vte._emapa_term_key = girs._emapa_term_key
	    and vte._stage_key = girs._stage_key
	    and struct._term_key = vte._term_key
	    and gs._Specimen_key >= %d
	    and gs._Specimen_key < %d
	''',
	]

# list of file definitions, each a 3-item tuple:
#	1. table name
#	2. list of fieldnames in order as sent to be written
#	3. list of fieldnames in order as they should actually be written
files = [
        ('assay_specimen',
		[ 'specimen_key', 'assay_key', 'genotype_key',
			'specimen_label', 'sex', 'age', 'fixation',
			'embedding_method', 'hybridization', 'age_note',
			'specimen_note', 'specimen_seq' ],
		[ 'specimen_key', 'assay_key', 'genotype_key',
			'specimen_label', 'sex', 'age', 'fixation',
			'embedding_method', 'hybridization', 'age_note', 
			'specimen_note', 'specimen_seq' ] ),

        ('specimen_result',
		[ 'specimen_result_key', 'specimen_key', 'structure',
			'structure_mgd_key', 'level', 'pattern', 'note',
			'specimen_result_seq' ],
                [ 'specimen_result_key', 'specimen_key', 'structure',
			'structure_mgd_key', 'level', 'pattern', 'note',
			'specimen_result_seq' ] ),

        ('specimen_result_to_imagepane',
		[ 'specimen_result_imagepane_key', 'specimen_result_key',
			'imagepane_key', 'imagepane_seq'],
                [ 'specimen_result_imagepane_key', 'specimen_result_key',
			'imagepane_key', 'imagepane_seq'] )
        ]


# global instance of a SpecimenGatherer
gatherer = SpecimenGatherer (files, cmds)
gatherer.setupChunking (
	'select min(_Specimen_key) from GXD_Specimen',
	'select max(_Specimen_key) from GXD_Specimen',
	10000
	)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
