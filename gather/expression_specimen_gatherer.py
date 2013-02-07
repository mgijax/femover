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

class SpecimenGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the expression_assay table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for assays,
	#	collates results, writes tab-delimited text file

	def collateResults(self):

		# process specimens +  results 
		specCols = [ 'specimen_key', 'assay_key', 'specimen_label' ]
                specRows = []

		resultCols = [ 'specimen_result_key', 'specimen_key', 'structure',
			'structure_mgd_key','level','pattern','note' ]
                resultRows = []


		(cols, rows) = self.results[0]

		# define specimen columns from mgd query
		assayKeyCol = Gatherer.columnNumber (cols, '_assay_key')
		specKeyCol = Gatherer.columnNumber (cols, '_specimen_key')
		specLabelCol = Gatherer.columnNumber (cols, 'specimenlabel')

		# define result columns from mgd query
		resultKeyCol = Gatherer.columnNumber (cols, '_result_key')
		structureCol = Gatherer.columnNumber (cols, 'printname')
		structureKeyCol = Gatherer.columnNumber (cols, '_structure_key')
		strengthCol = Gatherer.columnNumber (cols, 'strength')
		patternCol = Gatherer.columnNumber (cols, 'pattern')
		resultNoteCol = Gatherer.columnNumber (cols, 'resultnote')
	
		uniqueSpecimenKeys = set()
		for row in rows:
			assayKey = row[assayKeyCol]
			specimenKey = row[specKeyCol]
			specimenLabel = row[specLabelCol]
			resultKey = row[resultKeyCol]
			structure = row[structureCol]
			structureMGDKey = row[structureKeyCol]
			# strength = detection level
			strength = row[strengthCol]
			pattern = row[patternCol]
			resultNote = row[resultNoteCol]

			# hide not specified pattern
			if pattern == 'Not Specified':
				pattern = ""

			if specimenKey not in uniqueSpecimenKeys:
				uniqueSpecimenKeys.add(specimenKey)
				# make a new specimen row
				specRows.append((specimenKey,assayKey,specimenLabel))

			# make a new specimen result row
			resultRows.append((resultKey,specimenKey,structure,structureMGDKey,strength,pattern,resultNote))

		# Add all the column and row information to the output
		self.output = [(specCols,specRows),(resultCols,resultRows)]
			
		return

	def postprocessResults(self):
		return

###--- globals ---###

cmds = [
	# 0. Gather all the specimens and their results 
	'''select gs._assay_key,gs._specimen_key,gs.specimenlabel,gir._result_key,struct.printname,struct._structure_key,str.strength,gp.pattern, gir.resultnote
	    from gxd_specimen gs, gxd_insituresult gir, gxd_isresultstructure girs, gxd_structure struct,gxd_strength str, gxd_pattern gp
	    where gs._specimen_key=gir._specimen_key
		and gir._result_key=girs._result_key
		and girs._structure_key=struct._structure_key
		and gir._strength_key=str._strength_key
		and gir._pattern_key=gp._pattern_key
		and gs._assay_key=4820
	''',
	]

files = [
        ('assay_specimen',
                [ 'specimen_key', 'assay_key', 'specimen_label' ],
                'assay_specimen'),

        ('specimen_result',
                [ 'specimen_result_key', 'specimen_key', 'structure',
                'structure_mgd_key','level','pattern','note' ],
                'specimen_result'),
        ]


# global instance of a SpecimenGatherer
gatherer = SpecimenGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
