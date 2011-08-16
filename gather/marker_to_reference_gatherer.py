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
		
		# first, build the list of marker/reference pairs for
		# strain-specific data

		columns, rows = self.results[1]
		markerKeyCol = Gatherer.columnNumber (columns, '_Marker_key')
		refsKeyCol = Gatherer.columnNumber (columns, '_Refs_key')

		strSpecific = {}
		for row in rows:
			strSpecific[(row[markerKeyCol], row[refsKeyCol])] = 1 

		# now process the main body of data

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

				if strSpecific.has_key ( (markerKey, first) ):
					specific = 1
				else:
					specific = 0

				row = [ markerKey, first, specific, 'earliest' ]
				rows.append (row)

				# do any refs between earliest and latest
				for ref in okayRefs[markerKey][1:-1]:
					if strSpecific.has_key ( (markerKey, ref) ):
						specific = 1
					else:
						specific = 0

					row = [ markerKey, ref, specific, None ]
					rows.append (row)

				# do latest ref
				last = okayRefs[markerKey][-1]
				if first != last:
					if strSpecific.has_key ( (markerKey, last) ):
						specific = 1
					else:
						specific = 0

					row = [ markerKey, last, specific, 'latest' ]
					rows.append (row)

				hasPublicRefs = True
			else:
				hasPublicRefs = False

			if privateRefs.has_key(markerKey):
				i = 0
				lastRef = len(privateRefs[markerKey]) - 1

				for ref in privateRefs[markerKey]:
					flag = 'private'
					if not hasPublicRefs:
						if i == 0:
							flag = 'earliest'
						elif i == lastRef:
							flag = 'latest'

					row = [ markerKey, ref, 0, flag ]
					rows.append (row)
					i = i + 1

		logger.debug ('Generated %d rows' % len(rows))
		self.finalResults = rows
		self.finalColumns = [ '_Marker_key', '_Refs_key', 'isStrainSpecific', 'qualifier']
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

	# get the set of all references with data for strain-specific markers
	'''select m._Object_key as _Marker_key, m._Refs_key
	from mgi_reference_assoc m, mgi_refassoctype t
	where t._refassoctype_key = 1028
	and m._MGIType_key = 2
	and m._refassoctype_key = t._refassoctype_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Marker_key', '_Refs_key', 'isStrainSpecific',
	'qualifier', ]

# prefix for the filename of the output file
filenamePrefix = 'marker_to_reference'

# global instance of a MarkerToReferenceGatherer
gatherer = MarkerToReferenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
