#!/usr/local/bin/python
# 
# gathers data for the 'marker_tissue_expression_counts' table in the
# front-end database

import Gatherer

###--- Classes ---###

class TissueCountGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_tissue_expression_counts table
	# Has: queries to execute against the source database
	# Does: queries the source database for counts of expression by tissue
	#	and marker, collates results, writes tab-delimited text file

	def collateResults (self):
		cols, rows = self.results[0]

		# list of ordered marker keys
		markerList = []	

		# marker key -> [ ordered structure keys ]
		markerStructures = {}

		# structure key -> (stage, print name)
		structures = {}

		# (marker key, structure key) -> count
		detected = {}

		# (marker key, structure key) -> count
		notDetected = {}

		# find the index of each column we will need

		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		structureKeyCol = Gatherer.columnNumber (cols,
			'_Structure_key')
		printNameCol = Gatherer.columnNumber (cols, 'printName')
		expressedCol = Gatherer.columnNumber (cols, 'expressed')
		stageCol = Gatherer.columnNumber (cols, 'stage')

		# walk through the results and fill our data structures

		lastMarkerKey = None
		lastStructureKey = None

		for row in rows:
			markerKey = row[markerKeyCol]
			structureKey = row[structureKeyCol]
			expressed = row[expressedCol]

			# compile ordered lists of markers and structures per
			# marker

			if markerKey != lastMarkerKey:
				markerList.append (markerKey)
				markerStructures[markerKey] = []
				lastMarkerKey = markerKey
				lastStructureKey = None

			if structureKey != lastStructureKey:
				markerStructures[markerKey].append (
					structureKey)
				lastStructureKey = structureKey

			# collect the stage and printName for each structure

			if not structures.has_key(structureKey):
				structures[structureKey] = (row[stageCol],
				    '%s: %s' % (row[stageCol],
					row[printNameCol].replace(';', '; ')))

			# add to the detected / not detected counts per
			# marker + structure pair

			if expressed:
				dict = detected
			else:
				dict = notDetected

			t = (markerKey, structureKey)
			if dict.has_key(t):
				dict[t] = dict[t] + 1
			else:
				dict[t] = 1

		# now collate all those data structures into our set of
		# final results

		self.finalColumns = [ '_Marker_key', '_Structure_key',
			'printName', 'allCount', 'detectedCount',
			'notDetectedCount', 'sequenceNum' ]
		self.finalResults = []

		# counter of rows generated so far (for ordering)
		i = 0

		for markerKey in markerList:
			for structureKey in markerStructures[markerKey]:
				i = i + 1
				detCount = 0
				notDetCount = 0
				(stage, printName) = structures[structureKey]

				t = (markerKey, structureKey)

				if detected.has_key(t):
					detCount = detected[t]
				if notDetected.has_key(t):
					notDetCount = notDetected[t]

				row = [ markerKey, structureKey, printName,
					detCount + notDetCount,
					detCount, notDetCount, i ]

				self.finalResults.append(row)
		return

###--- globals ---###

cmds = [ '''select ge._Marker_key,
		gs._Structure_key,
		gs.printName,
		ge.expressed,
		gt.stage,
		gs.topoSort
	from gxd_expression ge,
		gxd_structure gs,
		gxd_theilerstage gt
	where ge.isForGXD = 1
		and ge._Structure_key = gs._Structure_key
		and gs._Stage_key = gt._Stage_key
	order by ge._Marker_key, gt.stage, gs.topoSort'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Structure_key', 'printName',
	'allCount', 'detectedCount', 'notDetectedCount', 'sequenceNum'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_tissue_expression_counts'

# global instance of a TissueCountGatherer
gatherer = TissueCountGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
