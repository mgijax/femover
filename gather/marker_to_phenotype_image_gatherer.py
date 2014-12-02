#!/usr/local/bin/python
# 
# gathers data for the 'marker_to_phenotype_image' table in the front-end db

import Gatherer

MUTATION_INVOLVES = 1003
EXPRESSES_COMPONENT = 1004

###--- Classes ---###

class MarkerToPhenoImageGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_to_phenotype_image table
	# Has: queries to execute against the source database
	# Does: queries the source database for markers that can be tied to
	#	phenotype images, collates results, writes tab-delimited text
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
		m._Marker_key
	from img_image i,
		img_imagepane p,
		img_imagepane_assoc a,
		all_allele aa,
		mrk_marker m
	where i._Image_key = p._Image_key
		and p._ImagePane_key = a._ImagePane_key
		and a._MGIType_key = 11
		and a._Object_key = aa._Allele_key
		and aa._Marker_key = m._Marker_key
	union
	select distinct i._Image_key,
		i._ThumbnailImage_key,
		i._Refs_key,
		r._Object_key_2 as _Marker_key
	from img_image i,
		img_imagepane p,
		img_imagepane_assoc a,
		mgi_relationship r
	where i._Image_key = p._Image_key
		and p._ImagePane_key = a._ImagePane_key
		and a._MGIType_key = 11
		and a._Object_key = r._Object_key_1
		and r._Category_key in (%d, %d)''' % (MUTATION_INVOLVES,
			EXPRESSES_COMPONENT)
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Image_key', '_Refs_key', 'qualifier'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_to_phenotype_image'

# global instance of a MarkerToPhenoImageGatherer
gatherer = MarkerToPhenoImageGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
