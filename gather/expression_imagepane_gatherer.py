#!/usr/local/bin/python
# 
# gathers data for the 'expression_imagepane' and 'expression_imagepane_set'
# tables in the front-end database

import Gatherer
import logger

###--- Classes ---###

class ExpressionImagePaneGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the expression_imagepane and
	#	expression_imagepane_set tables
	# Has: queries to execute against the source database
	# Does: queries the source database for image panes associated with
	#	expression assays, collates results, writes tab-delimited text
	#	files

	def cacheMarkers (self):
		cols, rows = self.results[0]

		keyCol = Gatherer.columnNumber (cols, '_Marker_key')
		symbolCol = Gatherer.columnNumber (cols, 'symbol')
		idCol = Gatherer.columnNumber (cols, 'accID')

		self.markers = {}
		for row in rows:
			self.markers[row[keyCol]] = (row[symbolCol],
				row[idCol])

		logger.debug('Cached data for %d markers' % len(self.markers))
		return

	def getAssays (self, resultIndex, label):
		cols, rows = self.results[resultIndex]

		imgKeyCol = Gatherer.columnNumber (cols, '_Image_key')
		assayKeyCol = Gatherer.columnNumber (cols, '_Assay_key')
		mrkKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		paneLabelCol = Gatherer.columnNumber (cols, 'paneLabel')
		idCol = Gatherer.columnNumber (cols, 'assayID')
		assayTypeCol = Gatherer.columnNumber (cols, 'assayType')
		xDimCol = Gatherer.columnNumber (cols, 'xDim')
		paneKeyCol = Gatherer.columnNumber (cols, '_ImagePane_key')

		assays = []
		for row in rows:
			imageKey = row[imgKeyCol]
			assayKey = row[assayKeyCol]
			markerKey = row[mrkKeyCol]
			paneLabel = row[paneLabelCol]
			assayID = row[idCol]
			assayType = row[assayTypeCol]

			if row[xDimCol]:
				inPixeldb = 1
			else:
				inPixeldb = 0

			# look up cached marker symbol and ID
			(symbol, markerID) = self.markers[markerKey]

			assays.append ( (imageKey, assayType, markerKey,
				paneLabel, assayKey, assayID, symbol,
				inPixeldb, markerID, row[paneKeyCol]) )
		logger.debug ('Collected %d %s assays' % (len(assays), label))

		assays.sort()
		logger.debug ('Sorted %d %s assays' % (len(assays), label))
		return assays

	def collateResults (self):
		self.cacheMarkers()
		gelAssays = self.getAssays (1, 'Gel')
		inSituAssays = self.getAssays (2, 'In Situ')
		allAssays = inSituAssays + gelAssays

		panes = []	# rows for expression_imagepane
		paneSets = []	# rows for expression_imagepane_set
		details = []	# rows for expression_imagepane_details

		# column names for rows in 'panes'
		paneCols = [ '_Image_key', 'paneLabel', '_ImagePane_key',
			'sequenceNum' ]

		# column names for rows in 'details'
		detailCols = [ '_ImagePane_key', 'markerSymbol', '_Assay_key',
			'assayID', '_Marker_key', 'markerID', 'sequenceNum' ]

		# column names for rows in 'paneSets'
		paneSetCols = [ '_Image_key', 'assayType', 'paneLabels',
			'_Marker_key', 'inPixeldb', 'sequenceNum' ]

		i = 0		# counter for ordering

		lastSet = None	# last image key, assay type, marker key tuple
		paneLabels = []	# current set of pane labels
		panesDone = {}	# image pane key -> 1

		# each row in allAssays defines a row for 'panes' and 
		# contributes to a row for 'paneSets' where a pane set is
		# defined for a unique (image key, assay type, marker key)
		# tuple
		for (imageKey, assayType, markerKey, paneLabel, assayKey,
			assayID, symbol, inPixeldb, markerID, paneKey) \
			in allAssays:

			i = i + 1
			if not panesDone.has_key (paneKey):
				# sequence num to be appended later:
				panes.append ( [imageKey, paneLabel, paneKey,
					] )
				panesDone[paneKey] = 1

			# sequence num to be appended later:
			details.append ( [ paneKey, symbol, assayKey, assayID,
				markerKey, markerID ] )

			imageAssayMarker = (imageKey, assayType, markerKey)

			# same set as last time, just add to the pane labels
			# and go back to the loop
			if imageAssayMarker == lastSet:
				if paneLabel and \
				    (paneLabel not in paneLabels):
					paneLabels.append (paneLabel)
				continue

			# encountered new set, save the old one
			if lastSet != None:
				if paneLabels:
					label = ', '.join (paneLabels)
				else:
					label = None

				oldImage, oldAssay, oldMarker = lastSet
				paneSets.append ( (oldImage, oldAssay,
					label, oldMarker, inPixeldb, i) )

			# both new sets and the initial set need to do basic
			# setup work:

			lastSet = imageAssayMarker
			if paneLabel:
				paneLabels = [ paneLabel ]
			else:
				paneLabels = []

		# the last pane set would not have been saved when the loop
		# ends, so save it now

		if lastSet:
			if paneLabels:
				label = ', '.join (paneLabels)
			else:
				label = None

			imageKey, assayType, markerKey = lastSet
			paneSets.append ( (imageKey, assayType,
				label, markerKey, inPixeldb, i) )

		details.sort()
		panes.sort()

		for dataset in [ details, panes ]:
			i = 0
			for row in dataset:
				i = i + 1
				row.append (i)
		logger.debug ('Added sequence numbers')

		self.output.append ( (paneCols, panes) )
		self.output.append ( (paneSetCols, paneSets) )
		self.output.append ( (detailCols, details) )
		return

###--- globals ---###

cmds = [
	# cache all markers cited in GXD assays
	'''select m._Marker_key, m.symbol, acc.accID
	from mrk_marker m,
		acc_accession acc,
		gxd_assay a
	where m._Marker_key = a._Marker_key
		and m._Marker_key = acc._Object_key
		and acc._MGIType_key = 2
		and acc._LogicalDB_key = 1
		and acc.preferred = 1''',

	# pick up images for gel assays
	'''select distinct i._Image_key,
		a._Assay_key,
		a._Marker_key,
		p.paneLabel,
		acc.accID as assayID,
		gat.assaytype,
		i.xDim,
		p._ImagePane_key
	from img_image i,
		img_imagepane p,
		gxd_assay a,
		acc_accession acc,
		gxd_assaytype gat
	where i._Image_key = p._Image_key
		and p._ImagePane_key = a._ImagePane_key
		and a._Assay_key = acc._Object_key
		and acc._MGIType_key = 8
		and acc.preferred = 1
		and a._AssayType_key = gat._AssayType_key
	order by a._Marker_key, i._Image_key, p.paneLabel''',

	# pick up images for in situ assays
	'''select distinct i._Image_key,
		a._Assay_key,
		a._Marker_key,
		p.paneLabel,
		acc.accID as assayID,
		gat.assayType,
		i.xDim,
		p._ImagePane_key
	from img_image i,
		img_imagepane p,
		gxd_assay a,
		acc_accession acc,
		gxd_insituresultimage g,
		gxd_insituresult r,
		gxd_specimen s,
		gxd_assaytype gat
	where i._Image_key = p._Image_key
		and p._ImagePane_key = g._ImagePane_key
		and g._Result_key = r._Result_key
		and r._Specimen_key = s._Specimen_key
		and s._Assay_key = a._Assay_key
		and a._Assay_key = acc._Object_key
		and acc._MGIType_key = 8
		and acc.preferred = 1
		and a._AssayType_key = gat._AssayType_key
	order by a._Marker_key, i._Image_key, p.paneLabel'''
	]

# order of fields (from the query results) to be written to the
# output file
files = [
	('expression_imagepane',
		[ '_ImagePane_key', '_Image_key', 'paneLabel', 
		'sequenceNum' ],
		'expression_imagepane'),

	('expression_imagepane_set',
		[ Gatherer.AUTO, '_Image_key', 'assayType', 'paneLabels',
		'_Marker_key', 'inPixeldb', 'sequenceNum' ],
		'expression_imagepane_set'),

	('expression_imagepane_details',
		[ Gatherer.AUTO, '_ImagePane_key', '_Assay_key',
		'assayID', '_Marker_key', 'markerID', 'markerSymbol',
		'sequenceNum' ],
		'expression_imagepane_details'),
	]

# global instance of a ExpressionImagePaneGatherer
gatherer = ExpressionImagePaneGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
