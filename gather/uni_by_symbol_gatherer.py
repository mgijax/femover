#!/usr/local/bin/python
# 
# gathers data for the 'uni_by_symbol' table in the front-end database

import Gatherer
import logger
import GXDUniUtils
import OutputFile

from expression_ht import experiments
GXDUniUtils.setExptIDList(experiments.getExperimentIDsAsList(True))

# fields we can order by
orderBy = [
	'by_marker',
	'by_assay_type',
	'ageMin',
	'ageMax',
	'by_structure',
	'_Stage_key',
	'is_detected desc',
	'uni_key'
]

# first - build the table of sorted results
sortedTable = GXDUniUtils.getSortedTable('sorted_table', ', '.join(orderBy))

###--- Classes ---###

class BySymbolGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the uni_by_symbol table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for both classical
	#	and RNA-Seq expression results, collates results, writes
	#	tab-delimited text file

	def __init__ (self, filenamePrefix, fieldOrder, cmds):
		Gatherer.ChunkGatherer.__init__(self, filenamePrefix, fieldOrder, cmds)
		self.chunkSize = 500000
		return

	def getOutputFile (self):
		return OutputFile.TrustingOutputFile (self.filenamePrefix)

	def getMinKeyQuery (self):
		return 'select min(uni_key) from %s' % sortedTable

	def getMaxKeyQuery (self):
		return 'select max(uni_key) from %s' % sortedTable

###--- globals ---###

cmds = [
	'''select uni_key, seqNum
		from %s
		where uni_key >= %%d
		and uni_key < %%d
		order by 1''' % sortedTable,
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'uni_key', 'seqNum',
	]

# prefix for the filename of the output file
filenamePrefix = 'uni_by_symbol'

# global instance of a BySymbolGatherer
gatherer = BySymbolGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
