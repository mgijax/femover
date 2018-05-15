#!/usr/local/bin/python
# 
# gathers data for the 'strain_disease' table in the front-end database

import Gatherer
import logger
import StrainUtils
import symbolsort

###--- Classes ---###

class StrainDiseaseGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the strain_disease table
	# Has: queries to execute against the source database
	# Does: queries the source database for diseases associated with strains,
	#	collates results, writes tab-delimited text file

	def compareRows(self, a, b):
		# sort rows first by strain key, then smart-alpha by disease term
		
		c = cmp(a[self.strainCol], b[self.strainCol])
		if c == 0:
			c = symbolsort.nomenCompare(a[self.termCol], b[self.termCol])
		return c
	
	def postprocessResults(self):
		# order the rows propertly and append a sequence number
		
		self.finalColumns.append('sequenceNum')
		self.convertFinalResultsToList()
		self.strainCol = Gatherer.columnNumber(self.finalColumns, '_Strain_key')
		self.termCol = Gatherer.columnNumber(self.finalColumns, 'term')
		self.finalResults.sort(self.compareRows)
		
		i = 0
		for row in self.finalResults:
			row.append(i)
			i = i + 1
		logger.debug('Added %d seq nums' % i)
		return
	
###--- globals ---###

cmds = [
	# 0. diseases for strains that we're moving to the front-end database
	'''select distinct gg._Strain_key, t._Term_key, t.term, aa.accID
		from voc_annot va, gxd_genotype gg, voc_term t, acc_accession aa, %s s
		where va._AnnotType_key = 1020
			and va._Qualifier_key = 1614158
			and va._Object_key = gg._Genotype_key
			and va._Term_key = t._Term_key
			and va._Term_key = aa._Object_key
			and aa._MGIType_key = 13
			and aa.preferred = 1
			and gg._Strain_key = s._Strain_key''' % StrainUtils.getStrainTempTable(),
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Strain_key', '_Term_key', 'accID', 'term', 'sequenceNum', ]

# prefix for the filename of the output file
filenamePrefix = 'strain_disease'

# global instance of a StrainDiseaseGatherer
gatherer = StrainDiseaseGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
