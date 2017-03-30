#!/usr/local/bin/python
# 
# gathers data for the 'mapping_id' table in the front-end database

import Gatherer
import symbolsort
import logger

###--- Globals ---###

EXPT_KEY_COL = None
ID_COL = None

###--- Functions ---###

def compareRows (a, b):
	# assumes EXPT_KEY_COL and ID_COL have been initialized; sorts first by key, then smart-alpha by ID
	
	k = cmp(a[EXPT_KEY_COL], b[EXPT_KEY_COL])
	if k:
		return k 
	return symbolsort.nomenCompare(a[ID_COL], b[ID_COL])
	
###--- Classes ---###

class MappingIDGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the mapping_id table
	# Has: queries to execute against the source database
	# Does: queries the source database for IDs for mapping experiments,
	#	collates results, writes tab-delimited text file

	def postprocessResults(self):
		global EXPT_KEY_COL, ID_COL
		
		self.convertFinalResultsToList()
		EXPT_KEY_COL = Gatherer.columnNumber(self.finalColumns, '_Object_key')
		ID_COL = Gatherer.columnNumber(self.finalColumns, 'accID')
		
		self.finalResults.sort(compareRows)
		i = 0
		for row in self.finalResults:
			i = i + 1
			row.append(i)
		self.finalColumns.append('sequenceNum')
		
		logger.debug('Ordered %d ID rows' % i)
		return
	
###--- globals ---###

cmds = [
	'''select a._Object_key, ldb.name, a.accID, a.preferred, a.private
		from acc_accession a, acc_logicaldb ldb, mld_expts e
		where a._MGIType_key = 4
		and a._Object_key = e._Expt_key
		and e.exptType != 'CONTIG'
		and a._LogicalDB_key = ldb._LogicalDB_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Object_key', 'name', 'accID', 'preferred', 'private', 'sequenceNum', ]

# prefix for the filename of the output file
filenamePrefix = 'mapping_id'

# global instance of a MappingIDGatherer
gatherer = MappingIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
