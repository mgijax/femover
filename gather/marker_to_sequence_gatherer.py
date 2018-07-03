#!/usr/local/bin/python
# 
# gathers data for the 'markerToSequence' table in the front-end database

import Gatherer
import logger
import dbAgnostic
import VocabUtils
import SequenceUtils

###--- Globals ---###

QUALIFIERS = {}		# maps from qualifier key to qualifier term (for performance, minimizing lookups via function calls)

###--- Classes ---###

class MarkerToSequenceGatherer (Gatherer.CachingMultiFileGatherer):
	# Is: a data gatherer for the marker_to_sequence table
	# Has: queries to execute against the source database
	# Does: queries the source database for marker/sequence
	#	relationships, collates results, writes tab-delimited text
	#	file

	def processResultSet(self, rsIndex):
		# process the set of results identified by integer 'rsIndex'
		
		cols, rows = self.results[rsIndex]
		
		markerCol = Gatherer.columnNumber(cols, '_Marker_key')
		sequenceCol = Gatherer.columnNumber(cols, '_Sequence_key')
		refsCol = Gatherer.columnNumber(cols, '_Refs_key')
		qualifierCol = Gatherer.columnNumber(cols, '_Qualifier_key')
		
		for row in rows:
			pair = (row[markerCol], row[sequenceCol])
			if pair not in self.pairsSeen:
				self.pairsSeen.add(pair)

				qualifierKey = row[qualifierCol]
				if qualifierKey not in QUALIFIERS:
					QUALIFIERS[qualifierKey] = VocabUtils.getTerm(qualifierKey)

				self.addRow('marker_to_sequence', [ pair[0], pair[1], row[refsCol], QUALIFIERS[qualifierKey] ])
		return	

	def collateResults(self):
		self.pairsSeen = set()		# set of (marker key, sequence key) pairs processed so far
		self.processResultSet(0)
		self.processResultSet(1)
		return 
	
###--- SQL commands ---###

cmds = [
	# 0-1. return marker/sequence associations based on the temp tables created in initialize()
	'''select smc._Marker_key, smc._Sequence_key, smc._Refs_key, smc._Qualifier_key
		from %s mrk, %s smc
		where mrk._Marker_key = smc._Marker_key
			and mrk.row_num >= %%d 
			and mrk.row_num < %%d''' % (SequenceUtils.getMarkersWithSequences(), SequenceUtils.getSequenceMarkerTable()),

	'''select msm._Marker_key, msm._Sequence_key, msm._Refs_key, msm._Qualifier_key
		from %s mrk, %s msm
		where mrk._Marker_key = msm._Marker_key
			and mrk.row_num >= %%d 
			and mrk.row_num < %%d''' % (SequenceUtils.getMarkersWithSequences(), SequenceUtils.getStrainMarkers()),
	]

files = [
	('marker_to_sequence',
		[ '_Marker_key', '_Sequence_key', '_Refs_key', 'qualifier' ],
		[ Gatherer.AUTO, '_Marker_key', '_Sequence_key', '_Refs_key', 'qualifier' ]
		)
	]

# global instance of a MarkerToSequenceGatherer
gatherer = MarkerToSequenceGatherer (files, cmds)
gatherer.setupChunking(
	'select min(row_num) from %s' % SequenceUtils.getMarkersWithSequences(),
	'select max(row_num) from %s' % SequenceUtils.getMarkersWithSequences(),
	50000
	)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
