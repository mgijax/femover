#!/usr/local/bin/python
# 
# gathers data for the 'markerToReference' table in the front-end database

import Gatherer
import PrivateRefSet
import logger

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

		# marker key -> list of okay (not de-emphasized) refs keys
		okayRefs = {}

		# marker key -> list of private (de-emphasized) refs keys
		privateRefs = {}

		# marker key -> 1
		markerKeys = {}

		for r in rows:
			markerKey = r[markerKeyCol]
			refsKey = r[refsKeyCol]

			if PrivateRefSet.isPrivate (refsKey):
				d = privateRefs
			else:
				d = okayRefs

			if d.has_key(markerKey):
				d[markerKey].append(refsKey)
			else:
				d[markerKey] = [ refsKey ]
				markerKeys[markerKey] = 1

		logger.debug ('Collected refs for %d markers (%d:%d)' % (
			len(markerKeys), len(okayRefs), len(privateRefs) ))

		# now go through the lists of references for each marker,
		# flagging the earliest, latest, and private references

		markers = markerKeys.keys()
		markers.sort()

		logger.debug ('Sorted %d marker keys' % len(markers))

		rows = []

		for markerKey in markers:
			if okayRefs.has_key(markerKey):
				# do earliest ref
				first = okayRefs[markerKey][0]
				row = [ markerKey, first, 'earliest' ]
				rows.append (row)

				# do any refs between earliest and latest
				for ref in okayRefs[markerKey][1:-1]:
					row = [ markerKey, ref, None ]
					rows.append (row)

				# do latest ref
				last = okayRefs[markerKey][-1]
				if first != last:
					row = [ markerKey, last, 'latest' ]
					rows.append (row)

			if privateRefs.has_key(markerKey):
				for ref in privateRefs[markerKey]:
					row = [ markerKey, ref, 'private' ]
					rows.append (row)

		logger.debug ('Generated %d rows' % len(rows))
		self.finalResults = rows
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
