#!/usr/local/bin/python
# 
# gathers data for the 'markerOrthology' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class MarkerOrthologyGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerOrthology table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for orthologies,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		cols, rows = self.results[0]

		mouseKeyCol = Gatherer.columnNumber (cols, 'mouseMarkerKey')
		otherKeyCol = Gatherer.columnNumber (cols, 'otherMarkerKey')
		otherSymbolCol = Gatherer.columnNumber (cols, 'otherSymbol')
		organismCol = Gatherer.columnNumber (cols, 'commonName')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')

		# byMouse[mouse marker key] -> [ [ priority value,
		#	other organism (lowercased), other organism,
		#	other marker key, other symbol ], ... ]
		byMouse = {}

		for row in rows:
			mouseKey = row[mouseKeyCol]
			otherOrganismKey = row[organismKeyCol]
			otherOrganism = row[organismCol]

			# To bring human and rat to the front of the list, we
			# use a smaller priority value.  Others are grouped
			# with the same priority value so they will be sorted
			# based on lowercased organism name.

			if otherOrganismKey == 1:
				priority = 0	# mouse -- should not happen
			elif otherOrganismKey == 2:
				priority = 1	# human
			elif otherOrganismKey == 40:
				priority = 2	# rat
			else:
				priority = 3	# all other organisms

			sortableRow = [ priority, otherOrganism.lower(),
				otherOrganism, row[otherKeyCol],
				row[otherSymbolCol] ]

			if byMouse.has_key(mouseKey):
				byMouse[mouseKey].append (sortableRow)
			else:
				byMouse[mouseKey] = [ sortableRow ]

		logger.debug ('Processed %d orthology rows' % len(rows))

		# now sort the rows for each marker and add them to the list
		# of output rows

		self.finalColumns = [ 'mouseMarkerKey', 'otherMarkerKey',
			'otherSymbol', 'otherOrganism', 'sequenceNum' ]
		self.finalResults = []

		keys = byMouse.keys()
		keys.sort()

		for key in keys:
			# sort the rows for this marker
			byMouse[key].sort()

			i = 0		# each marker gets its own counter

			for row in byMouse[key]:
				i = i + 1
				self.finalResults.append ( [ key,
					row[3], row[4], row[2], i ] )
		logger.debug ('Got %d orthology rows for %d markers' % (
			len(self.finalResults), len(byMouse)) )
		return

###--- globals ---###

cmds = [
	'''select distinct mouse._Marker_key as mouseMarkerKey,
		nonmouse._Marker_key as otherMarkerKey,
		mm.symbol as otherSymbol,
		nonmouse._Organism_key,
		org.commonName
	from mrk_homology_cache mouse,
		mrk_homology_cache nonmouse,
		mrk_marker mm,
		mgi_organism org
	where mouse._Organism_key = 1
		and mouse._Class_key = nonmouse._Class_key
		and nonmouse._Organism_key != 1
		and exists (select 1 from mrk_marker m
			where m._Marker_key = nonmouse._Marker_key)
		and nonmouse._Organism_key = org._Organism_key
		and nonmouse._Marker_key = mm._Marker_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO,
	'mouseMarkerKey', 'otherMarkerKey', 'otherSymbol', 'otherOrganism',
	'sequenceNum',
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_orthology'

# global instance of a MarkerOrthologyGatherer
gatherer = MarkerOrthologyGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
