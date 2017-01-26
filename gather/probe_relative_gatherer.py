#!/usr/local/bin/python
# 
# gathers data for the 'probe_relative' table in the front-end database

import Gatherer
import symbolsort
import logger

###--- Globals ---###

keyCol = None
nameCol = None
idCol = None

###--- Functions ---###

def probeCompare (a, b):
	# Compare probe rows first by probe key, then smart-alpha by probe name.  This will ensure
	# that related probes for any given probe are together and are sorted appropriately.  Fall
	# back on a final sort by related probe ID, though we shouldn't really get to it (unless
	# there are two related probes with duplicate names).
	
	k = cmp(a[keyCol], b[keyCol])
	if k:
		return k
	
	n = symbolsort.nomenCompare(a[nameCol], b[nameCol])
	if n:
		return n
	
	return symbolsort.nomenCompare(a[idCol], b[idCol])

###--- Classes ---###

class ProbeRelativeGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the probe_relative table
	# Has: queries to execute against the source database
	# Does: queries the source database for derived-from and derivative relationships for
	#	probes, collates results, writes tab-delimited text file

	def postprocessResults (self):
		global keyCol, nameCol, idCol

		self.convertFinalResultsToList()

		keyCol = Gatherer.columnNumber(self.finalColumns, '_Probe_key')
		nameCol = Gatherer.columnNumber(self.finalColumns, 'name')
		idCol = Gatherer.columnNumber(self.finalColumns, 'accID')

		self.finalResults.sort(probeCompare)
		logger.debug('sorted %d probes' % len(self.finalResults))
		
		self.finalColumns.append('sequence_num')
		i = 0
		for row in self.finalResults:
			i = i + 1
			row.append(i)
			
		logger.debug('Added %d sequence numbers' % i)
		return 

###--- globals ---###

cmds = [
	# 0. top of union retrieves parents (probes from which current probe was derived),
	#	bottom retrieves children (probes derived from current probe).  Ordered to bring
	#	all relatives of a given probe together.
	'''select p._Probe_key, r._Probe_key as related_probe_key, r.name, a.accID, 0 as is_child
		from prb_probe p, prb_probe r, acc_accession a
		where p.derivedFrom = r._Probe_key
			and r._Probe_key = a._Object_key
			and a._LogicalDB_key = 1
			and a._MGIType_key = 3
			and a.preferred = 1
		union
		select p._Probe_key, r._Probe_key as related_probe_key, r.name, a.accID, 1 as is_child
		from prb_probe p, prb_probe r, acc_accession a
		where p._Probe_key = r.derivedFrom
			and r._Probe_key = a._Object_key
			and a._LogicalDB_key = 1
			and a._MGIType_key = 3
			and a.preferred = 1
		order by 1'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_probe_key', 'related_probe_key', 'accID', 'name', 'is_child', 'sequence_num',
	]

# prefix for the filename of the output file
filenamePrefix = 'probe_relative'

# global instance of a ProbeRelativeGatherer
gatherer = ProbeRelativeGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
