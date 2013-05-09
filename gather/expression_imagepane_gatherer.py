#!/usr/local/bin/python
# 
# gathers data for the 'expression_imagepane' and 'expression_imagepane_set'
# tables in the front-end database

import Gatherer
import logger
import SymbolSorter

###--- Classes ---###

class ExpressionImagePaneGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the expression_imagepane and
	#	expression_imagepane_set tables
	# Has: queries to execute against the source database
	# Does: queries the source database for image panes associated with
	#	expression assays, collates results, writes tab-delimited text
	#	files

	imagePaneSortFields = []
	imagePaneSeqMap = {}
	assayTypeSeqMap = {}

	def initSeqMaps(self):
		self.assayTypeSeqMap = {
			6:1, # immuno
			1:2, # RNA In situ
			9:3, # in situ knockin
			2:4, # northern blot
			8:5, # western blot
			5:6, # RT-PCR
			4:7, # RNase Protection
			3:8, # Nuclease S1
		}
			
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
		assayTypeKeyCol = Gatherer.columnNumber (cols, '_assaytype_key')
		mrkKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		paneLabelCol = Gatherer.columnNumber (cols, 'paneLabel')
		paneXCol = Gatherer.columnNumber (cols, 'x')
		paneYCol = Gatherer.columnNumber (cols, 'y')
		paneWidthCol = Gatherer.columnNumber (cols, 'width')
		paneHeightCol = Gatherer.columnNumber (cols, 'height')
		idCol = Gatherer.columnNumber (cols, 'assayID')
		assayTypeCol = Gatherer.columnNumber (cols, 'assayType')
		xDimCol = Gatherer.columnNumber (cols, 'xDim')
		paneKeyCol = Gatherer.columnNumber (cols, '_ImagePane_key')
		thumbKeyCol = Gatherer.columnNumber (cols,
			'_ThumbnailImage_key')
		citationCol = Gatherer.columnNumber (cols,'short_citation')

		assays = []

		for row in rows:
			imageKey = row[imgKeyCol]
			assayKey = row[assayKeyCol]
			markerKey = row[mrkKeyCol]
			paneLabel = row[paneLabelCol]
			paneX = row[paneXCol] or 0
			paneY = row[paneYCol] or 0
			paneWidth = row[paneWidthCol] or 0
			paneHeight = row[paneHeightCol] or 0
			assayID = row[idCol]
			assayType = row[assayTypeCol]

			if row[xDimCol]:
				inPixeldb = 1
			else:
				inPixeldb = 0

			# look up cached marker symbol and ID
			(symbol, markerID) = self.markers[markerKey]

			assays.append ( (imageKey, assayType, markerKey,
				paneLabel, paneX, paneY, paneWidth, paneHeight, assayKey, assayID, symbol,
				inPixeldb, markerID, row[paneKeyCol],
				row[thumbKeyCol] ) )
			self.registerImagePaneSortFields(row[paneKeyCol],row[assayTypeKeyCol],symbol,row[citationCol])

		logger.debug ('Collected %d %s assays' % (len(assays), label))

		assays.sort()
		logger.debug ('Sorted %d %s assays' % (len(assays), label))

		return assays

	def getIDs (self, panes):
		# get the columns and rows for the table of IDs; ensure that
		# we only include IDs for keys in the given set of 'panes'

		# distinct set of unique pane keys
		paneKeys = {}
		for [ imageKey, paneLabel, paneKey, paneX, paneY, paneWidth, paneHeight, byDefault] in panes:
			paneKeys[paneKey] = 1

		cols, rows = self.results[3]

		keyCol = Gatherer.columnNumber (cols, '_Object_key')
		idCol = Gatherer.columnNumber (cols, 'accID')
		privateCol = Gatherer.columnNumber (cols, 'private')
		preferredCol = Gatherer.columnNumber (cols, 'preferred')
		logicalDbCol = Gatherer.columnNumber (cols, 'name')

		outputCols = [ '_Object_key', 'logicalDb', 'accID',
				'preferred', 'private', 'sequence_num' ]
		outputRows = []
		seqNum = 0

		for row in rows:
			seqNum = seqNum + 1
			outputRows.append ( [ row[keyCol], row[logicalDbCol],
				row[idCol], row[preferredCol],
				row[privateCol], seqNum ] ) 
		return outputCols, outputRows


	# keep track of the different sort fields for gxd image panes.
	# it goes assay type, then symbol (for the highest assay type), then ref citation
	# because of the weird symbol rule, we have to track symbol_seqs for each assay type
	def registerImagePaneSortFields(self,paneKey,assay_type_key,symbol,ref_citation):
		# set a large seq num if we have an un-mapped assay type
		assay_type_seq = assay_type_key in self.assayTypeSeqMap and self.assayTypeSeqMap[assay_type_key] or 99
		symbol_seq = SymbolSorter.getGeneSymbolSeq(symbol) 
		seq_map = self.imagePaneSeqMap.setdefault(paneKey,{'citation':ref_citation})
		seq_map.setdefault(assay_type_seq,[]).append(symbol_seq)

	def sortImagePaneFields(self):
		tempPaneSeqMap = {}
		tempPaneSortFields = []
		for paneKey,obj in self.imagePaneSeqMap.items():
			ref_citation = ""
			if "citation" in obj:
				ref_citation = obj["citation"]
				del obj["citation"]
			best_assay_seq = min(obj.keys())
			best_symbol_seq = min(obj[best_assay_seq])
			tempPaneSortFields.append((paneKey,best_assay_seq,best_symbol_seq,ref_citation))
		
		# sort by best assay_type, best symbol (in that assay type), then by ref citation
		tempPaneSortFields.sort(key=lambda x: (x[1],x[2],x[3]))
		count = 0
		for sortField in tempPaneSortFields:
			count += 1
			tempPaneSeqMap[sortField[0]] = count

		self.imagePaneSeqMap = tempPaneSeqMap

		logger.debug("sorted image panes by default sequence")

	def collateResults (self):
		self.cacheMarkers()
		self.initSeqMaps()

		gelAssays = self.getAssays (1, 'Gel')
		inSituAssays = self.getAssays (2, 'In Situ')
		allAssays = inSituAssays + gelAssays

		# sort the imagePane fields
		self.sortImagePaneFields()

		panes = []	# rows for expression_imagepane
		paneSets = []	# rows for expression_imagepane_set
		details = []	# rows for expression_imagepane_details

		# column names for rows in 'panes'
		paneCols = [ '_Image_key', 'paneLabel', '_ImagePane_key','x','y','width','height','by_default',
			'sequenceNum' ]

		# column names for rows in 'details'
		detailCols = [ '_ImagePane_key', 'markerSymbol', '_Assay_key',
			'assayID', '_Marker_key', 'markerID', 'sequenceNum' ]

		# column names for rows in 'paneSets'
		paneSetCols = [ '_Image_key', '_ThumbnailImage_key',
			'assayType', 'paneLabels', '_Marker_key', 'inPixeldb',
			'sequenceNum' ]

		i = 0		# counter for ordering

		lastSet = None	# last image key, assay type, marker key tuple
		paneLabels = []	# current set of pane labels
		panesDone = {}	# image pane key -> 1

		# each row in allAssays defines a row for 'panes' and 
		# contributes to a row for 'paneSets' where a pane set is
		# defined for a unique (image key, assay type, marker key)
		# tuple
		for (imageKey, assayType, markerKey, paneLabel, paneX, paneY, paneWidth, paneHeight, assayKey,
			assayID, symbol, inPixeldb, markerID, paneKey,
			thumbKey) in allAssays:

			i = i + 1
			if not panesDone.has_key (paneKey):
				# sequence num to be appended later:
				panes.append ( [imageKey, paneLabel, paneKey, paneX,paneY,paneWidth,paneHeight,
					self.imagePaneSeqMap[paneKey]] )
				panesDone[paneKey] = 1

			# sequence num to be appended later:
			details.append ( [ paneKey, symbol, assayKey, assayID,
				markerKey, markerID ] )

			imageAssayMarker = (imageKey, thumbKey, assayType,
				markerKey)

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

				oldImage, oldThumb, oldAssay, oldMarker = \
					lastSet
				paneSets.append ( (oldImage, oldThumb,
					oldAssay, label, oldMarker, inPixeldb,
					i) )

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

			imageKey, thumbKey, assayType, markerKey = lastSet
			paneSets.append ( (imageKey, thumbKey, assayType,
				label, markerKey, inPixeldb, i) )

		details.sort()
		panes.sort()

		idCols, idRows = self.getIDs(panes)

		for dataset in [ details, panes ]:
			i = 0
			for row in dataset:
				if row[0] == 1002:
					logger.debug("row for 1002 = %s"%row)
				i = i + 1
				row.append (i)
		logger.debug ('Added sequence numbers')

		self.output.append ( (paneCols, panes) )
		self.output.append ( (paneSetCols, paneSets) )
		self.output.append ( (detailCols, details) )
		self.output.append ( (idCols, idRows) )
		return

###--- globals ---###

cmds = [
	# 0. cache all markers cited in GXD assays
	'''select m._Marker_key, m.symbol, acc.accID
	from mrk_marker m,
		acc_accession acc,
		gxd_assay a
	where exists (select 1 from gxd_expression e where a._Assay_key = e._Assay_key)
		and m._Marker_key = a._Marker_key
		and m._Marker_key = acc._Object_key
		and acc._MGIType_key = 2
		and acc._LogicalDB_key = 1
		and acc.preferred = 1''',

	# 1. pick up images for gel assays
	'''select distinct i._Image_key,
		a._Assay_key,
		a._assaytype_key,
		a._Marker_key,
		p.paneLabel,
		p.x,p.y,p.width,p.height,
		acc.accID as assayID,
		gat.assaytype,
		i.xDim,
		p._ImagePane_key,
		i._ThumbnailImage_key,
		bcc.short_citation
	from img_image i,
		img_imagepane p,
		gxd_assay a,
		acc_accession acc,
		gxd_assaytype gat,
		bib_citation_cache bcc
	where exists (select 1 from gxd_expression e where a._Assay_key = e._Assay_key)
		and i._Image_key = p._Image_key
		and p._ImagePane_key = a._ImagePane_key
		and a._Assay_key = acc._Object_key
		and acc._MGIType_key = 8
		and acc.preferred = 1
		and a._AssayType_key = gat._AssayType_key
		and bcc._refs_key=a._refs_key
	order by a._Marker_key, i._Image_key, p.paneLabel''',

	# 2. pick up images for in situ assays
	'''select distinct i._Image_key,
		a._Assay_key,
		a._assaytype_key,
		a._Marker_key,
		p.paneLabel,
		p.x,p.y,p.width,p.height,
		acc.accID as assayID,
		gat.assayType,
		i.xDim,
		p._ImagePane_key,
		i._ThumbnailImage_key,
		bcc.short_citation
	from img_image i,
		img_imagepane p,
		gxd_assay a,
		acc_accession acc,
		gxd_insituresultimage g,
		gxd_insituresult r,
		gxd_specimen s,
		gxd_assaytype gat,
		bib_citation_cache bcc
	where exists (select 1 from gxd_expression e where a._Assay_key = e._Assay_key)
		and i._Image_key = p._Image_key
		and p._ImagePane_key = g._ImagePane_key
		and g._Result_key = r._Result_key
		and r._Specimen_key = s._Specimen_key
		and s._Assay_key = a._Assay_key
		and a._Assay_key = acc._Object_key
		and acc._MGIType_key = 8
		and acc.preferred = 1
		and a._AssayType_key = gat._AssayType_key
		and bcc._refs_key=a._refs_key
	order by a._Marker_key, i._Image_key, p.paneLabel''',

	# 3. get accession IDs for image panes

	'''select a._Object_key,
		a.accID,
		a.private, 
		a.preferred, 
		d.name, 
		a.numericPart
	from acc_accession a, 
		acc_logicaldb d
	where a._MGIType_key = 35
		and a._LogicalDB_key = d._LogicalDB_key
	order by a._Object_key, a.numericPart''', 
	]

# order of fields (from the query results) to be written to the
# output file
files = [
	('expression_imagepane',
		[ '_ImagePane_key', '_Image_key', 'paneLabel','x','y','width','height','by_default', 
		'sequenceNum' ],
		'expression_imagepane'),

	('expression_imagepane_set',
		[ Gatherer.AUTO, '_Image_key', '_ThumbnailImage_key', 
		'assayType', 'paneLabels', '_Marker_key', 'inPixeldb',
		'sequenceNum' ],
		'expression_imagepane_set'),

	('expression_imagepane_details',
		[ Gatherer.AUTO, '_ImagePane_key', '_Assay_key',
		'assayID', '_Marker_key', 'markerID', 'markerSymbol',
		'sequenceNum' ],
		'expression_imagepane_details'),

	('expression_imagepane_id',
		[ Gatherer.AUTO, '_Object_key', 'logicalDb', 'accID',
		'preferred', 'private', 'sequence_num' ],
		'expression_imagepane_id'),
	]

# global instance of a ExpressionImagePaneGatherer
gatherer = ExpressionImagePaneGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
