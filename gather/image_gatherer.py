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

			if self.myThumb.has_key(key):
				thumb = self.myThumb[key]
			else:
				thumb = None

			if self.myFull.has_key(key):
				full = self.myFull[key]
			else:
				full = None

			if thumb is not None:
				isThumb = 0
			else:
				isThumb = 1

			self.addColumn ('thumbnailKey', thumb, row,
				self.finalColumns)
			self.addColumn ('fullsizeKey', full, row,
				self.finalColumns)
			self.addColumn ('isThumbnail', isThumb, row,
				self.finalColumns)
			self.addColumn ('imageType', Gatherer.resolve (
				row[typeCol]), row, self.finalColumns)
		return
		
###--- globals ---###

cmds = [
	'''select _Image_key, _ThumbnailImage_key
		from IMG_Image
		where _ThumbnailImage_key is not null''',

	'''select _Image_key, xDim, yDim, _Refs_key, figureLabel,
			_ImageType_key
		from img_image''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Image_key', '_Refs_key', 'thumbnailKey', 'fullsizeKey',
		'isThumbnail', 'xDim', 'yDim', 'figureLabel', 'imageType',
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
