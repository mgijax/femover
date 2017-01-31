#!/usr/local/bin/python
# 
# gathers data for the 'probe_reference_sequence' table in the front-end database

import Gatherer
import logger
import OutputFile

###--- Classes ---###

class SimpleCachingGatherer:
	# Is: a gatherer that makes it easy to retrieve data from the server in batches, process it,
	#	cache a portion of the output in memory, while automatically writing to a file periodically
	#	to keep memory requirements down
	# Has:	1. a single query
	#		2. a single output file (eg- a single table produced)
	
	def __init__ (self, tableName, inFieldOrder, outFieldOrder, cmds, chunkSize = 10000, cacheSize = 10000):
		self.nextAutoKey = None
		self.tableName = tableName
		self.inFieldOrder = inFieldOrder
		self.outFieldOrder = outFieldOrder
		
		if type(cmds) == type([]):
			self.cmds = cmds
		else:
			self.cmds = [ cmds ]
			
		self.chunkSize = chunkSize
		self.cacheSize = cacheSize
		self.files = OutputFile.CachingOutputFileFactory()
		self.offset = 0
		
		if type(cmds) == type([]):
			if len(cmds) != 1:
				raise Exception("SimpleCachingGatherer can only accept a single SQL command")
		return
	
	def go (self):
		self.outputFileID = self.files.createFile(self.tableName, self.inFieldOrder, self.outFieldOrder, self.cacheSize)
		logger.debug("Set up CachingOutputFile %d" % self.outputFileID)

		self.results = Gatherer.executeQueries(self.chunkQuery())
		while (len(self.results[0][1]) > 0):
			self.collateResults()
			logger.debug('Handled rows %d to %d' % (self.offset, self.offset + len(self.results[0][1])) )
			self.offset = self.offset + self.chunkSize
			self.results = Gatherer.executeQueries(self.chunkQuery())
		
		self.files.closeAll()
		self.files.reportAll()
		return
	
	def chunkQuery(self):
		return '%s limit %d offset %d' % (self.cmds[0], self.chunkSize, self.offset)

	def addRow(self, row):
		self.files.addRow(self.outputFileID, row)
		
	def addRows(self, rows):
		self.files.addRows(self.outputFileID, rows)

	def collateResults(self):
		raise Exception("collateResults must be defined in subclass")

class PRSGatherer (SimpleCachingGatherer):
	# Is: a data gatherer for the probe_reference_sequence table
	# Has: queries to execute against the source database
	# Does: queries the source database for sequences associated with probe/reference
	#	pairs, collates results, writes tab-delimited text file

	def collateResults(self):
		(cols, rows) = self.results[0]
		
		refKeyCol = Gatherer.columnNumber(cols, '_Reference_key')
		seqIdCol = Gatherer.columnNumber(cols, 'seqID')
		seqKeyCol = Gatherer.columnNumber(cols, '_Sequence_key')

		for row in rows:
			self.addRow((row[refKeyCol], row[seqKeyCol], row[seqIdCol], None))
		return

###--- globals ---###

cmds = [
	# 0. step through references based on probe key, as that's the clustered index for PRB_Reference
	'''select r._Reference_key, pa.accID as seqID, sa._Object_key as _Sequence_key
		from prb_reference r
		inner join acc_accessionreference ar on (ar._Refs_key = r._Refs_key)
		inner join acc_accession pa on (pa._Accession_key = ar._Accession_key
			and pa._MGIType_key = 3
			and pa._LogicalDB_key = 9
			and pa._Object_key = r._Probe_key)
		inner join acc_accession sa on (pa.accID = sa.accID
			and sa._LogicalDB_key = 9
			and sa._MGIType_key = 19)'''
	]

# global instance of a PRSGatherer
gatherer = PRSGatherer ('probe_reference_sequence', 
		[ '_Reference_key', '_Sequence_key', 'seqID', 'qualifier' ],
		[ Gatherer.AUTO, '_Reference_key', '_Sequence_key', 'seqID', 'qualifier' ],
		cmds,
		chunkSize = 350000)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
