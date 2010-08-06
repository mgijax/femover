#!/usr/local/bin/python
# 
# gathers data for the 'probeToSequence' table in the front-end database

import Gatherer

###--- Classes ---###

class ProbeToSequenceGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the probeToSequence table
	# Has: queries to execute against the source database
	# Does: queries the source database for probe/sequence
	#	relationships, collates results, writes tab-delimited text
	#	file

	def postprocessResults (self):
		self.convertFinalResultsToList()

		for row in self.finalResults:
			self.addColumn ('qualifier', None, row,
				self.finalColumns)
		return 

###--- globals ---###

cmds = [
	'''select _Probe_key,
		_Sequence_key,
		_Refs_key
	from seq_probe_cache''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Probe_key', '_Sequence_key', '_Refs_key',
	'qualifier'
	]

# prefix for the filename of the output file
filenamePrefix = 'probeToSequence'

# global instance of a ProbeToSequenceGatherer
gatherer = ProbeToSequenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
