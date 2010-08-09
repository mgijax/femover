#!/usr/local/bin/python
# 
# gathers data for the 'probe' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class ProbeGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the probe table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for probes,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		
		# produce a cache with the clone ID for each probe which has
		# one

		columns, rows = self.results[1]

		keyCol = Gatherer.columnNumber (columns, '_Probe_key')
		idCol = Gatherer.columnNumber (columns, 'cloneID')

		cloneIDs = {}
		for r in rows:
			cloneIDs[r[keyCol]] = r[idCol]

		logger.debug('Cached IDs for %d clones' % len(cloneIDs))

		# remember the cache for post-processing

		self.cloneIDs = cloneIDs

		# the first query contains the bulk of the data we need, with
		# the rest to come via post-processing

		self.finalColumns = self.results[0][0]
		self.finalResults = self.results[0][1]
		return

	def postprocessResults (self):
		# Purpose: override of standard method for key-based lookups

		self.convertFinalResultsToList()

		keyCol = Gatherer.columnNumber (self.finalColumns,
			'_Probe_key') 
		ldbCol = Gatherer.columnNumber (self.finalColumns,
			'_LogicalDB_key')

		for r in self.finalResults:
			self.addColumn ('logicalDB', Gatherer.resolve (
				r[ldbCol], 'acc_logicaldb', '_LogicalDB_key',
				'name'), r, self.finalColumns)

			probeKey = r[keyCol]
			if self.cloneIDs.has_key (probeKey):
				self.addColumn ('cloneID',
					self.cloneIDs[probeKey],
					r, self.finalColumns)
			else:
				self.addColumn ('cloneID', None, r,
					self.finalColumns)
		return

###--- globals ---###

cmds = [
	'''select p._Probe_key,
		p.name,
		t.term as segmentType,
		a.accID as primaryID,
		a._LogicalDB_key
	from prb_probe p, voc_term t, acc_accession a
	where p._Probe_key = a._Object_key
		and a._MGIType_key = 3
		and a.preferred = 1
		and a._LogicalDB_key = 1
		and p._SegmentType_key = t._Term_key''',

	'''select p._Probe_key, a.accID as cloneID
	from prb_probe p, acc_accession a, mgi_setmember msm, mgi_set ms
	where p._Probe_key = a._Object_key
		and a._MGIType_key = 3
		and a._LogicalDB_key = msm._Object_key
		and msm._Set_key = ms._Set_key
		and ms.name = 'Clone Collection (all)' ''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Probe_key', 'name', 'segmentType', 'primaryID', 'logicalDB',
		'cloneID',
	]

# prefix for the filename of the output file
filenamePrefix = 'probe'

# global instance of a ProbeGatherer
gatherer = ProbeGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
