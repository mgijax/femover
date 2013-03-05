#!/usr/local/bin/python
# 
# gathers data for the 'gellane' (and attached tables) in the front-end database
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
NOT_SPECIFIED_SIZE = "Size Not Specified"
NOT_SPECIFIED = "Not Specified"

class GelLaneGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the expression_assay table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for assays,
	#	collates results, writes tab-delimited text file
	
	# build all the lane structure relationships
	# sort them here if GXD decides they want to
	def getLaneStructures(self):
		sCols = [ 'unique_key','gellane_key', 'mgd_structure_key', 'printname',
                        'structure_seq']
		sRows = []

		(cols, rows) = self.results[1]
		
		laneKeyCol = Gatherer.columnNumber (cols, '_gellane_key')
		stageKeyCol = Gatherer.columnNumber (cols, '_stage_key')
		printnameCol = Gatherer.columnNumber (cols, 'printname')
		structureKeyCol = Gatherer.columnNumber (cols, '_structure_key')

		rowCount = 0
		for row in rows:
			rowCount += 1
			laneKey = row[laneKeyCol]
			stageKey = row[stageKeyCol]
			printname = row[printnameCol]
			structureKey = row[structureKeyCol]

			# structure format is TS26: brain
			tsStructure = "TS%s: %s"%(stageKey,printname)

			# TODO: if GXD wants these sorted, you would need to do that in here
			sRows.append((rowCount,laneKey,structureKey,tsStructure,1))
	
		return (sCols,sRows)

	def collateResults(self):

		# process gel lanes + bands 
		laneCols = [ 'gellane_key', 'assay_key', 'genotype_key','sex',
                        'age','age_note','lane_note','sample_amount',
                        'lane_label','control_text','is_control',
                        'rna_type','lane_seq']
                laneRows = []
		bandCols = ['gelband_key','gellane_key','gelrow_key','assay_key',
                        'rowsize','row_note','strength','band_note',
                        'row_seq']
		bandRows = []

		(cols, rows) = self.results[0]

		# define gel lane columns from mgd query
		assayKeyCol = Gatherer.columnNumber (cols, '_assay_key')
		laneKeyCol = Gatherer.columnNumber (cols, '_gellane_key')
		genotypeKeyCol = Gatherer.columnNumber (cols, '_genotype_key')
		#conditionalGenotypeCol = Gatherer.columnNumber (cols, 'conditional_genotype')
		laneSeqCol = Gatherer.columnNumber (cols, 'lane_seq')
		laneLabelCol = Gatherer.columnNumber (cols, 'lanelabel')
		sampleAmountCol = Gatherer.columnNumber (cols, 'sampleamount')
		laneNoteCol = Gatherer.columnNumber (cols, 'lanenote')
		sexCol = Gatherer.columnNumber (cols, 'sex')
		ageCol = Gatherer.columnNumber (cols, 'age')
		ageNoteCol = Gatherer.columnNumber (cols, 'agenote')
		isControlCol = Gatherer.columnNumber (cols, 'is_control')
		controlTextCol = Gatherer.columnNumber (cols, 'gellanecontent')
		rnaTypeCol = Gatherer.columnNumber (cols, 'rnatype')

		# define the band related columns
		rowKeyCol = Gatherer.columnNumber (cols, '_gelrow_key')
		rowNoteCol = Gatherer.columnNumber (cols, 'rownote')
		rowSeqCol = Gatherer.columnNumber (cols, 'row_seq')
		rowSizeCol = Gatherer.columnNumber (cols, 'size')
		rowUnitsCol = Gatherer.columnNumber (cols, 'units')
		bandKeyCol = Gatherer.columnNumber (cols, '_gelband_key')
		bandNoteCol = Gatherer.columnNumber (cols, 'bandnote')
		bandStrengthCol = Gatherer.columnNumber (cols, 'strength')

	
		uniqueLaneKeys = set()
		uniqueBandKeys = set()
		for row in rows:
			assayKey = row[assayKeyCol]
			laneKey = row[laneKeyCol]
			genotypeKey = row[genotypeKeyCol]
			#isConditionalGenotype = row[conditionalGenotypeCol]
			laneSeq = row[laneSeqCol]
			laneLabel = row[laneLabelCol]
			sampleAmount = row[sampleAmountCol]
			laneNote = row[laneNoteCol]
			sex = row[sexCol]
			age = row[ageCol]
			ageNote = row[ageNoteCol]
			isControl = row[isControlCol]
			controlText = row[controlTextCol]
			rnaType = row[rnaTypeCol]

			rowKey = row[rowKeyCol] 
			rowNote = row[rowNoteCol] 
			rowSeq = row[rowSeqCol] 
			rowSize = row[rowSizeCol] 
			rowUnits = row[rowUnitsCol] 
			bandKey = row[bandKeyCol] 
			bandNote = row[bandNoteCol] 
			bandStrength = row[bandStrengthCol] 

			# add conditional genotype note, if applicable
			#if isConditionalGenotype == 1:
			#	specimenNote = specimenNote and "%s %s"%(CONDITIONAL_GENOTYPE_NOTE,specimenNote) or CONDITIONAL_GENOTYPE_NOTE

			if laneKey not in uniqueLaneKeys:
				uniqueLaneKeys.add(laneKey)
				# make a new gellane row
				# display Not Specified for null sample amounts
				sampleAmount = sampleAmount==None and NOT_SPECIFIED or "%s &micro;g"%sampleAmount
				sampleAmountDisplay = "%s; %s RNA"%(sampleAmount,rnaType)

				laneRows.append((laneKey,assayKey,genotypeKey,sex,
					age,ageNote,laneNote,sampleAmountDisplay,laneLabel,
					controlText,isControl,rnaType,laneSeq))

			if bandKey not in uniqueBandKeys:
				uniqueBandKeys.add(bandKey)
				# make a new gelband row
				# when the amount(size) is Null the display is going to say something like 'Size Not Specified'
				rowSizeDisplay = rowSize==None and NOT_SPECIFIED_SIZE or "%s %s"%(rowSize,rowUnits) 

				bandRows.append((bandKey,laneKey,rowKey,assayKey,
					rowSizeDisplay,rowNote,bandStrength,bandNote,rowSeq))

		# Add all the column and row information to the output
		self.output = [(laneCols,laneRows),(bandCols,bandRows),
			self.getLaneStructures()]
			
		return

	def postprocessResults(self):
		return

###--- globals ---###

cmds = [
	# 0. Gather all the gellanes and their bands
	'''
	select gl._gellane_key,
                gl._assay_key,
                gl._genotype_key,
                gl.sequencenum lane_seq,
                gl.lanelabel,
                gl.sampleamount,
                gl.sex,
                gl.age,
                gl.agenote,
                gl.lanenote,
                CASE WHEN gl._gelcontrol_key=1 THEN 0
                        ELSE 1
                END is_control,
                glc.gellanecontent,
                glrna.rnatype,
	    gr.size,
	    gr.sequencenum row_seq,
	    gr.rownote,
	    gr._gelrow_key,
	   gunits.units,
	   gb.bandnote,
	   gb._gelband_key,
	   gstr.strength
        from gxd_gellane gl,
                gxd_gelcontrol glc,
                gxd_gelrnatype glrna,
	   gxd_gelband gb,
	   gxd_gelrow gr,
	  gxd_gelunits gunits,
	  gxd_strength gstr
        where gl._gelcontrol_key=glc._gelcontrol_key
                and gl._gelrnatype_key=glrna._gelrnatype_key
	   and gunits._gelunits_key=gr._gelunits_key
	   and gb._gellane_key=gl._gellane_key
	   and gb._gelrow_key=gr._gelrow_key
	  and gb._strength_key=gstr._strength_key
	''',
	# 1. Gather all the lane structures
	'''
	select s.printName,
		gs._gellane_key,
		gs._structure_key,
		s._stage_key
	from GXD_GelLaneStructure gs,
		GXD_Structure s
	where gs._Structure_key = s._Structure_key
	''',
	]

files = [
        ('gellane',
		[ 'gellane_key', 'assay_key', 'genotype_key','sex',
			'age','age_note','lane_note','sample_amount',
			'lane_label','control_text','is_control',
			'rna_type',
			'lane_seq' ],
                'gellane'),
	('gelband',
		['gelband_key','gellane_key','gelrow_key','assay_key',
			'rowsize','row_note','strength','band_note',
			'row_seq'],
		'gelband'),
        ('gellane_to_structure',
		[ 'unique_key','gellane_key', 'mgd_structure_key', 'printname',
			'structure_seq'],
                'gellane_to_structure'),
        ]


# global instance of a GelLaneGatherer
gatherer = GelLaneGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
