#!/usr/local/bin/python
# 
# This gatherer loads incidental mutation data file links for markers and alleles
#
# Author: kstone - March 2014
#

import Gatherer
import dbAgnostic
import config
import logger

###--- Constants ---###

###---- Functions ---###
def createTempTables():
        logger.debug("done creating temp tables")

###--- Classes ---###
class IncidentalMutationsGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for mp_annotation table 
	# Has: queries to execute against the source database

	masterFile=[]
	###--- Program Flow ---###
	def readMasterFile(self):
		try:
		    fp = open(config.INCIDENTAL_MUTS_FILE)
		    self.masterFile = [r.split("\t") for r in fp.readlines()]
		except:
			logger.debug("Could not open Incidental Mutations file.\n No data will be loaded")
			
		logger.debug("done reading input file")
	
	def buildMarkerLinks(self):
		cols=["unique_key","marker_key","filename"]

		# gen the marker ID lookup {mgiid=>markerKey}
		markerLookup = {}
		for r in self.results[1][1]:
			markerLookup[r[1]]=r[0]

		rows=[]
		uniqueRows=set([])
		rowCount=0
		# read the input rows to determine links
		for r in self.masterFile:
			rowCount+=1
			if len(r)<2:
				continue
			file=r[0]
			mgiid=r[1]
			uniqueRow=(file,mgiid)
			if uniqueRow in uniqueRows:
				continue
			uniqueRows.add(uniqueRow)
			# check mgiid existence
			if mgiid not in markerLookup:
				logger.debug("cannot identify marker for row %d: %s"%(rowCount,"\t".join(r)))
				continue

			rows.append((rowCount,markerLookup[mgiid],file))
		return cols,rows

	def buildAlleleLinks(self):
		cols=["unique_key","allele_key","filename"]

		# gen the allele ID lookup {mgiid=>alleleKey}
		alleleLookup = {}
		for r in self.results[0][1]:
			alleleLookup[r[1]]=r[0]

		rows=[]
		uniqueRows=set([])
		rowCount=0
		# read the input rows to determine links
		for r in self.masterFile:
			rowCount+=1
			if len(r)<3:
				continue
			file=r[0]
			mgiid=r[2]
			uniqueRow=(file,mgiid)
			if uniqueRow in uniqueRows:
				continue
			uniqueRows.add(uniqueRow)
			# check mgiid existence
			if mgiid not in alleleLookup:
				logger.debug("cannot identify allele for row %d: %s"%(rowCount,"\t".join(r)))
				continue

			rows.append((rowCount,alleleLookup[mgiid],file))
		return cols,rows

	# here is defined the high level script for building all these tables
	# by gatherer convention we create tuples of (cols,rows) for every table we want to create, and append them to self.output
	def buildRows (self):
		logger.debug("reading input master file")
		self.readMasterFile()		

		logger.debug("building marker incidental mutation links")
		markerCols,markerRows=self.buildMarkerLinks()

		logger.debug("building allele incidental mutation links")
		alleleCols,alleleRows=self.buildAlleleLinks()

		logger.debug("done. outputing to file")
		self.output=[(markerCols,markerRows),(alleleCols,alleleRows)]

	# this is a function that gets called for every gatherer
	def collateResults (self):
		# process all queries
		self.buildRows()

###--- MGD Query Definitions---###
# all of these queries get processed before collateResults() gets called
cmds = [
	# 0. Get all valid allele MGI Ids
	'''
	select av._object_key allele_key, av.accid allele_id
	from all_acc_view av
	where _logicaldb_key=1 
	''',
	# 1. Get all valid marker MGI Ids
	'''
	select mv._object_key marker_key, mv.accid marker_id
	from mrk_acc_view mv
	where _logicaldb_key=1 
	'''
	]

###--- Table Definitions ---###
# definition of output files, each as:
#	(filename prefix, list of fieldnames, table name)
files = [
	('marker_incidental_mut',
		['unique_key','marker_key','filename'],
		'marker_incidental_mut'),
	('allele_incidental_mut',
		['unique_key','allele_key','filename'],
		'allele_incidental_mut'),
	]

createTempTables()
# global instance of a AnnotationGatherer
gatherer = IncidentalMutationsGatherer (files, cmds)

###--- main program ---###
# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)

