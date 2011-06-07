#!/usr/local/bin/python
# 
# gathers data for the 'marker_biotype_conflict' table in the front-end db

import Gatherer

###--- Classes ---###

class BiotypeGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_biotype_conflict table
	# Has: queries to execute against the source database
	# Does: queries the source database for biotype conflicts for markers,
	#	collates results, writes tab-delimited text file

	def postprocessResults (self):
		self.convertFinalResultsToList()

		keyCol = Gatherer.columnNumber (self.finalColumns,
			'_Marker_key')

		keyCount = {}	# marker key -> count of rows so far

		for r in self.finalResults:
			key = r[keyCol]

			if keyCount.has_key(key):
				keyCount[key] = 1 + keyCount[key]
			else:
				keyCount[key] = 1

			self.addColumn ('sequenceNum', keyCount[key], r,
				self.finalColumns)
		return 

###--- globals ---###

cmds = [ '''select distinct s._Marker_key, s.accID, ldb.name, s.rawbiotype,
		s._LogicalDB_key
	from seq_marker_cache s, acc_logicaldb ldb
	where s._LogicalDB_key = ldb._LogicalDB_key
		and s._LogicalDB_key in (85,60,59)
		and exists (select 1 from seq_marker_cache t
			where s._Marker_key = t._Marker_key
			and t._BiotypeConflict_key = 5420767)
	order by s._Marker_key, s._LogicalDB_key desc''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', 'accID', 'name', 'rawbiotype',
	'sequenceNum',
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_biotype_conflict'

# global instance of a BiotypeGatherer
gatherer = BiotypeGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
