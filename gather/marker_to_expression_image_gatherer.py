#!/usr/local/bin/python
# 
# gathers data for the 'marker_to_expression_image' table in the front-end db

import Gatherer

###--- Classes ---###

class MarkerToExpressionImageGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_to_expression_image table
	# Has: queries to execute against the source database
	# Does: queries the source database for markers that can be tied to
	#	expression images, collates results, writes tab-delimited text
	#	file

	def collateResults (self):
		self.finalColumns = [ '_Marker_key', '_Image_key',
			'_Refs_key', 'qualifier' ]
		self.finalResults = []

		cols, rows = self.results[0]
		imgKeyCol = Gatherer.columnNumber (cols, '_Image_key')
		thumbKeyCol = Gatherer.columnNumber (cols,
			'_ThumbnailImage_key')
		refsKeyCol = Gatherer.columnNumber (cols, '_Refs_key')
		mrkKeyCol = Gatherer.columnNumber (cols, '_Marker_key')

		# need to add a row for the full-size image and a row for the
		# thumbnail image
		for row in rows:
			mrkKey = row[mrkKeyCol]
			refsKey = row[refsKeyCol]
			imgKey = row[imgKeyCol]
			thumbKey = row[thumbKeyCol]

			if imgKey:
				self.finalResults.append ( [ mrkKey, imgKey,
					refsKey, 'Full Size' ] )
			if thumbKey:
				self.finalResults.append ( [ mrkKey, thumbKey,
					refsKey, 'Thumbnail' ] )
		return

###--- globals ---###

cmds = [ '''select distinct i._Image_key,
		i._ThumbnailImage_key, 
		i._Refs_key,
		i._Object_key as _Marker_key
	from img_cache i
	where i._MGIType_key = 2
		and exists (select 1 from img_image ii
			where ii._Image_key = i._Image_key)
		and i._ImageMGIType_key = 8''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Image_key', '_Refs_key', 'qualifier'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_to_expression_image'

# global instance of a MarkerToExpressionImageGatherer
gatherer = MarkerToExpressionImageGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
