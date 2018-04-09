#!/usr/local/bin/python
# 
# gathers data for the 'sequenceID' table in the front-end database

import Gatherer
import logger
import dbAgnostic

###--- Functions ---###

def initialize():
	cmd0 = '''select row_number() over (order by a._Object_key, a._LogicalDB_key, a.accID) as row_num,
				a._Object_key as sequenceKey, a._LogicalDB_key, a.accID, a.preferred, a.private
		into temp table sequences
		from acc_accession a
		where a._MGIType_key = 19
			and exists (select 1 from seq_sequence ss
				where a._Object_key = ss._Sequence_key)
		order by a._Object_key, a._LogicalDB_key, a.accID'''
	
	cmd1 = 'create index seqIdx1 on sequences (row_num)'
	cmd2 = 'create index seqIdx3 on sequences (_LogicalDB_key)'
	
	for cmd in [ cmd0, cmd1, cmd2 ]:
		dbAgnostic.execute(cmd)
		
	logger.debug('built temp table of seq IDs')
	return

###--- Classes ---###

class SequenceIDGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the sequenceID table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for sequence IDs,
	#	collates results, writes tab-delimited text file

	def getMinKeyQuery (self):
		return 'select min(row_num) from sequences'

	def getMaxKeyQuery (self):
		return 'select max(row_num) from sequences'

###--- globals ---###

cmds = [
	'''select s.sequenceKey, ldb.name as logicalDB, s.accID, s.preferred, s.private
		from sequences s, acc_logicaldb ldb
		where s.row_num >= %d and s.row_num < %d
			and s._LogicalDB_key = ldb._LogicalDB_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'sequenceKey', 'logicalDB', 'accID',
	'preferred', 'private' ]

# prefix for the filename of the output file
filenamePrefix = 'sequence_id'

# global instance of a SequenceIDGatherer
gatherer = SequenceIDGatherer (filenamePrefix, fieldOrder, cmds)
gatherer.setChunkSize(500000)
initialize()

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
