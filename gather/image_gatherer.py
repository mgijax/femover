#!/usr/local/bin/python
# 
# gathers data for the 'image' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class ImageGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the image table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for images,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# query 0 has the fullsize to thumbnail mapping; extract it
		# and cache it for post-processing

		# myFull[thumbnail key] = full size key
		self.myFull = {}

		# myThumb[full size key] = thumbnail key
		self.myThumb = {}

		(columns, rows) = self.results[0]

		thumbCol = Gatherer.columnNumber (columns,
			'_ThumbnailImage_key')
		fullCol = Gatherer.columnNumber (columns, '_Image_key')

		for row in rows:
			thumb = row[thumbCol]
			full = row[fullCol]

			self.myFull[thumb] = full
			self.myThumb[full] = thumb

		logger.debug ('Found %d thumbnails' % len(self.myThumb))

		# cache the numeric part of the pixeldb ID for each image
		# from query 1

		self.pixID = {}		# pixID[image key] = numeric pix ID

		(columns, rows) = self.results[1]
		keyCol = Gatherer.columnNumber (columns, '_Object_key')
		idCol = Gatherer.columnNumber (columns, 'numericPart')

		for row in rows:
			self.pixID[row[keyCol]] = row[idCol]

		# cache the copyright statement (1023) and caption (1024)
		# for each image from query 2

		self.copyright = {}	# copyright[image key] = statement
		self.caption = {}	# caption[image key] = statement

		(columns, rows) = self.results[2]
		keyCol = Gatherer.columnNumber (columns, '_Object_key')
		typeCol = Gatherer.columnNumber (columns, '_NoteType_key')
		noteCol = Gatherer.columnNumber (columns, 'note')

		for row in rows:
			if row[typeCol] == 1023:
				dict = self.copyright
			else:
				dict = self.caption

			key = row[keyCol]
			if dict.has_key(key):
				dict[key] = dict[key] + row[noteCol]
			else:
				dict[key] = row[noteCol]

		# the last query has the bulk of the output data set

		self.finalColumns = self.results[-1][0]
		self.finalResults = self.results[-1][1]
		return

	def postprocessResults (self):

		self.convertFinalResultsToList()

		keyCol = Gatherer.columnNumber (self.finalColumns,
			'_Image_key')
		typeCol = Gatherer.columnNumber (self.finalColumns,
			'_ImageType_key')

		for row in self.finalResults:
			key = row[keyCol]

			thumb = None
			full = None
			isThumb = 1
			id = None
			copyright = None
			caption = None

			if self.myThumb.has_key(key):
				thumb = self.myThumb[key]

			if self.myFull.has_key(key):
				full = self.myFull[key]

			if thumb is not None:
				isThumb = 0

			if self.pixID.has_key(key):
				id = self.pixID[key]

			if self.copyright.has_key(key):
				copyright = self.copyright[key]

			if self.caption.has_key(key):
				caption = self.caption[key]

			self.addColumn ('thumbnailKey', thumb, row,
				self.finalColumns)
			self.addColumn ('fullsizeKey', full, row,
				self.finalColumns)
			self.addColumn ('isThumbnail', isThumb, row,
				self.finalColumns)
			self.addColumn ('imageType', Gatherer.resolve (
				row[typeCol]), row, self.finalColumns)
			self.addColumn ('pixNumeric', id, row,
				self.finalColumns)
			self.addColumn ('copyright', copyright, row,
				self.finalColumns)
			self.addColumn ('caption', caption, row,
				self.finalColumns)
		return
		
###--- globals ---###

cmds = [
	'''select _Image_key, _ThumbnailImage_key
		from IMG_Image
		where _ThumbnailImage_key is not null''',

	'''select _Object_key, numericPart
		from acc_accession
		where _MGIType_key = 9 and _LogicalDB_key = 19''',

	'''select n._Object_key, nc.note, n._NoteType_key, nc.sequenceNum
		from mgi_note n, mgi_notechunk nc
		where n._NoteType_key in (1023,1024)
			and n._Note_key = nc._Note_key
		order by nc.sequenceNum''',

	'''select _Image_key, xDim, yDim, _Refs_key, figureLabel,
			_ImageType_key
		from img_image''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Image_key', '_Refs_key', 'thumbnailKey', 'fullsizeKey',
		'isThumbnail', 'xDim', 'yDim', 'figureLabel', 'imageType',
		'pixNumeric', 'copyright', 'caption',
	]

# prefix for the filename of the output file
filenamePrefix = 'image'

# global instance of a ImageGatherer
gatherer = ImageGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)