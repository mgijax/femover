#!/usr/local/bin/python
# 
# gathers data for the 'markerToReference' table in the front-end database

import Gatherer

###--- Classes ---###

class MarkerToReferenceGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerToReference table
	# Has: queries to execute against the source database
	# Does: queries the source database for marker/reference
	#	associations, collates results, writes tab-delimited text file

	def collateResults (self):
		columns, rows = self.results[0]
		markerKeyCol = Gatherer.columnNumber (columns, '_Marker_key')
		refsKeyCol = Gatherer.columnNumber (columns, '_Refs_key')

		lastMarkerKey = None
		lastRow = None

		self.finalResults = []

		for r in rows:
			markerKey = r[markerKeyCol]
			refsKey = r[refsKeyCol]

			if markerKey != lastMarkerKey:
				qualifier = 'earliest'
				if lastRow and (not lastRow[-1]):
					lastRow[-1] = 'latest'
			else:
				qualifier = None

			row = [ markerKey, refsKey, qualifier ]
			self.finalResults.append (row)

			lastMarkerKey = markerKey
			lastRow = row

		if lastRow and (not lastRow[-1]):
			lastRow[-1] = 'latest'

		self.finalColumns = [ '_Marker_key', '_Refs_key', 'qualifier']
		return

###--- globals ---###

cmds = [
	# ordered (by year and numeric jnum) list of refs for each marker, so
	# we can find the earliest and latest
	'''select distinct mr._Marker_key, r.year, mr.jnum, mr._Refs_key
	from mrk_reference mr, mrk_marker m, bib_refs r
	where mr._Marker_key = m._Marker_key
		and mr._Refs_key = r._Refs_key
		and m._Marker_Status_key != 2
	order by mr._Marker_key, r.year, mr.jnum''',

#	'''select distinct mr._Marker_key, mr._Refs_key, '' as qualifier
#	from mrk_reference mr, mrk_marker m 
#	where mr._Marker_key = m._Marker_key
#		and m._Marker_Status_key != 2''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Marker_key', '_Refs_key', 'qualifier', ]

# prefix for the filename of the output file
filenamePrefix = 'marker_to_reference'

# global instance of a MarkerToReferenceGatherer
gatherer = MarkerToReferenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
