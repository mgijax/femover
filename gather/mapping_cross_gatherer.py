#!/usr/local/bin/python
# 
# gathers data for the 'mapping_cross' table in the front-end database

import Gatherer
import logger

###--- Globals ---###

# mapping from code stored in database for cross type to string for display to users
CROSS_TYPES = {
	'1'   : 'Backcross, sexes unspecified or combined',
	'3'   : 'Backcross, sexes unspecified or combined',
	'11'  : 'Backcross, female',
	'31'  : 'Backcross, female',
	'12'  : 'Backcross, male',
	'32'  : 'Backcross, male',
	'14'  : 'Intercross, treated like backcross(i.e. # chromosomes scored)',
	'4'   : 'Intercross',
	'5'   : 'Single backcross',
	'6'   : 'Other',
	'Uns' : 'Unspecified',
	}

###--- Functions ---###

def combine(a, b):
	# concatenate two strings a and b into a single value; handle nulls appropriately
	
	if a:
		if b:
			return a + b
		return a
	return None

def getCrossType(t):
	if t in CROSS_TYPES:
		return CROSS_TYPES[t]
	return None

###--- Classes ---###

class MappingCrossGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the mapping_cross table
	# Has: queries to execute against the source database
	# Does: queries the source database for additional data for CROSS mapping experiments,
	#	collates results, writes tab-delimited text file

	def postprocessResults(self):
		self.convertFinalResultsToList()
		
		female1Col = Gatherer.columnNumber(self.finalColumns, 'female')
		female2Col = Gatherer.columnNumber(self.finalColumns, 'female2')
		male1Col = Gatherer.columnNumber(self.finalColumns, 'male')
		male2Col = Gatherer.columnNumber(self.finalColumns, 'male2')
		whoseCrossCol = Gatherer.columnNumber(self.finalColumns, 'whoseCross')
		typeCol = Gatherer.columnNumber(self.finalColumns, 'type')
		matrixCountCol = Gatherer.columnNumber(self.finalColumns, 'matrix_count')
		
		# We'll need to compute and populate five additional columns for each row.

		self.finalColumns = self.finalColumns + [ 'crossType', 'femaleParent', 'maleParent', 'panelName', 'panelFilename' ]

		for row in self.finalResults:
			row.append(getCrossType(row[typeCol]))
			row.append(combine(row[female1Col], row[female2Col]))
			row.append(combine(row[male1Col], row[male2Col]))
			
			whoseCross = row[whoseCrossCol]
			filename = None
			
			if whoseCross:
				if row[matrixCountCol] > 0:
					filename = whoseCross.replace(' ', '_').replace('(', '').replace(')', '') + '_Panel.rpt'
				
			row.append(whoseCross)
			row.append(filename)
			
		logger.debug('Finished postprocessing %d results' % len(self.finalResults))
		return
	
###--- globals ---###

cmds = [
	'''with matrix_counts as (select c._Cross_key, count(distinct m._Marker_key) as matrix_count
			from crs_cross c
			left outer join crs_matrix m on (c._Cross_key = m._Cross_key)
			group by 1
		)
		select m._Expt_key, m.female, m.female2, m.male, m.male2, c.type, c.whoseCross, c.abbrevHO, c.abbrevHT,
			s1.strain as femaleStrain, s2.strain as maleStrain,
			s3.strain as homozygousStrain, s4.strain as heterozygousStrain, cm.matrix_count
		from MLD_Matrix m
		inner join CRS_Cross c on (m._Cross_key = c._Cross_key)
		inner join PRB_Strain s1 on (c._femaleStrain_key = s1._Strain_key)
		inner join PRB_Strain s2 on (c._maleStrain_key = s2._Strain_key)
		inner join PRB_Strain s3 on (c._StrainHO_key = s3._Strain_key)
		inner join PRB_Strain s4 on (c._StrainHT_key = s4._Strain_key)
		inner join matrix_counts cm on (c._Cross_key = cm._Cross_key)''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Expt_key', 'crossType', 'femaleParent', 'femaleStrain', 'maleParent', 'maleStrain',
	'panelName', 'panelFilename', 'abbrevHO', 'homozygousStrain', 'abbrevHT', 'heterozygousStrain',
	]

# prefix for the filename of the output file
filenamePrefix = 'mapping_cross'

# global instance of a MappingCrossGatherer
gatherer = MappingCrossGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
