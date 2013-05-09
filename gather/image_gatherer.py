#!/usr/local/bin/python
# 
# gathers data for the 'image' table in the front-end database

import Gatherer
import logger
import TagConverter
import dbAgnostic
import re

###--- Classes ---###

class ImageGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the image table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for images,
	#	collates results, writes tab-delimited text file

	def getElsevierCitation (self, jnum):
		# look up the citation for the given Elsevier J#

		if self.elsevier.has_key(jnum):
			return self.elsevier[jnum]

		cols, rows = dbAgnostic.execute (
			'''select r.journal, r.vol, r.pgs, r.authors, 
				r.title, r.year
			from bib_refs r,
				acc_accession a
			where r._Refs_key = a._Object_key
				and a.accID = '%s'
				and a._MGIType_key = 1''' % jnum)

		journalCol = dbAgnostic.columnNumber (cols, 'journal')
		volCol = dbAgnostic.columnNumber (cols, 'vol')
		pgsCol = dbAgnostic.columnNumber (cols, 'pgs')
		authorsCol = dbAgnostic.columnNumber (cols, 'authors')
		titleCol = dbAgnostic.columnNumber (cols, 'title')
		yearCol = dbAgnostic.columnNumber (cols, 'year')

		r = rows[0]
		citation = '%s %s: %s, %s, %s Copyright %s' % (
			r[journalCol], r[volCol], r[pgsCol],
			r[authorsCol], r[titleCol], r[yearCol] )

		self.elsevier[jnum] = citation
		logger.debug ('Retrieved %s' % jnum)
		return citation

	def convertElsevierTag (self, s):
		# convert any \Elsevier(...) tags in 's' to their respective
		# citations.

		# short-circuit for cases where the tag does not appear in 's'
		tag = '\\Elsevier('
		if s.find(tag) == -1:
			return s

		match = self.elsevierRE.search(s)

		# recurse to this method, just in case there are more than one
		# \Elsevier() tags in s

		if not match:
			return s

		return self.convertElsevierTag (s.replace (match.group(0),
			self.getElsevierCitation (match.group(1)) ) ) 

	def collateResults (self):
		# query 0 has the fullsize to thumbnail mapping; extract it
		# and cache it for post-processing

		# myFull[thumbnail key] = full size key
		self.myFull = {}

		# myThumb[full size key] = thumbnail key
		self.myThumb = {}

		# self.elsevier[J: num] = Elsevier citation
		self.elsevier = {}

		# full tag in group(0), J# in group(1)
		self.elsevierRE = re.compile ('\\\\Elsevier\(([^|)]+)\|\|\)?')

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

		# cache the 
		# copyright statement (1023) 
		# caption (1024)
		# exteran_link (1039)
		# for each image from query 2

		self.copyright = {}	# copyright[image key] = statement
		self.caption = {}	# caption[image key] = statement
		self.external_link = {}	# external_link[image key] = statement

		(columns, rows) = self.results[2]
		keyCol = Gatherer.columnNumber (columns, '_Object_key')
		typeCol = Gatherer.columnNumber (columns, '_NoteType_key')
		noteCol = Gatherer.columnNumber (columns, 'note')

		for row in rows:
			if row[typeCol] == 1023:
				dict = self.copyright
			elif row[typeCol] == 1024:
				dict = self.caption
			elif row[typeCol] == 1039:
				dict = self.external_link

			key = row[keyCol]
			if dict.has_key(key):
				dict[key] = dict[key] + row[noteCol]
			else:
				dict[key] = row[noteCol]

		# cache the MGI ID for each image from query 3

		self.mgiID = {}		# mgiID[image key] = MGI ID

		(columns, rows) = self.results[3]
		keyCol = Gatherer.columnNumber (columns, '_Object_key')
		idCol = Gatherer.columnNumber (columns, 'accID')

		for row in rows:
			self.mgiID[row[keyCol]] = row[idCol]

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
		classCol = Gatherer.columnNumber (self.finalColumns,
			'_ImageClass_key')

		for row in self.finalResults:
			key = row[keyCol]

			thumb = None
			full = None
			isThumb = 1
			id = None
			copyright = None
			caption = None
			external_link = None
			mgiID = None

			if self.myThumb.has_key(key):
				thumb = self.myThumb[key]

			if self.myFull.has_key(key):
				full = self.myFull[key]

			if thumb is not None:
				isThumb = 0

			if self.pixID.has_key(key):
				id = self.pixID[key]

			if self.mgiID.has_key(key):
				mgiID = self.mgiID[key]

			# trim trailing whitespace from notes fields

			if self.copyright.has_key(key):
				copyright = self.convertElsevierTag (
					self.copyright[key].rstrip() )

			if self.caption.has_key(key):
				caption = self.caption[key].rstrip()
				caption = TagConverter.convert(caption)

			if self.external_link.has_key(key):
				external_link = self.external_link[key].rstrip()

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
			self.addColumn ('mgiID', mgiID, row,
				self.finalColumns)
			self.addColumn ('copyright', copyright, row,
				self.finalColumns)
			self.addColumn ('caption', caption, row,
				self.finalColumns)
			self.addColumn ('external_link', external_link, row,
				self.finalColumns)
			self.addColumn ('imageClass',
				Gatherer.resolve (row[classCol]), row,
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
		where n._NoteType_key in (1023,1024,1039)
			and n._Note_key = nc._Note_key
		order by nc.sequenceNum''',

	'''select _Object_key, accID
		from acc_accession
		where _MGIType_key = 9 and _LogicalDB_key = 1
			and preferred = 1''',

	# this works because img_image._Refs_key is non-null in mgd
	'''select i._Image_key, i.xDim, i.yDim, i._Refs_key, i.figureLabel,
			i._ImageType_key, r.jnumID, i._ImageClass_key
		from img_image i, bib_citation_cache r
		where i._Refs_key = r._Refs_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Image_key', '_Refs_key', 'thumbnailKey', 'fullsizeKey',
		'isThumbnail', 'xDim', 'yDim', 'figureLabel', 'imageType',
		'pixNumeric', 'mgiID', 'jnumID', 'copyright', 'caption',
		'external_link', 'imageClass',
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
