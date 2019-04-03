#!/usr/local/bin/python
# 
# gathers data for the accession_* tables in the front-end database:
#	accession, accession_logical_db, accession_display_type,
#	accession_object_type

# Historical:
# This gatherer is more custom than the other Gatherers, because of the size
# of the data set.  It would be good to use a MultiFileGatherer because we are
# producing multiple tables, but it would also be good to use a ChunkGatherer
# because of the size of the data set for the main "accession" table.  So,
# this will be an entirely custom Gatherer, drawing ideas from them both.
#
# Present:
# This gatherer has been largely gutted, but for sake of time, I'm not 
# restructuring this presently.  See History note below for what's left.
#
# History
#
# 10/11/2018	jsb
#	largely gutted most of the data handled by this gatherer, leaving only those
#	pieces that could not be easily picked up from other places (ie- markers for
#	InterPro IDs and non-mouse gene IDs that should link to homology pages)
# 06/03/2015	jsb
#	added support for genotype IDs
# 04/29/2015	jsb
#	selectively added garbage collection calls
# 03/07/2013    lec
#       TR11248/commented out fillConsensusSnps, fillSubSnps (SNP accessions)

import Gatherer
import logger
import OutputFile
import dbAgnostic
import gc

###--- Globals ---###

Error = Gatherer.Error

LDB_FILE = 'accession_logical_db'
OBJECT_TYPE_FILE = 'accession_object_type'
ACCESSION_FILE = 'accession'
DISPLAY_TYPE_FILE = 'accession_display_type'

ORTHOLOGY_TYPE = 18
HGNC = 64

###--- Functions ---###

SEQNUM = {}		# ID suffix -> count of objects with that ID suffix
def sequenceNum (accID):
	global SEQNUM

	# final four characters of the ID will split our sequence numbers up
	# into roughly 10,000 bins.  Should help prevent overflowing the max
	# value for the sequence number.
	suffix = accID[-4:]

	if SEQNUM.has_key(suffix):
		SEQNUM[suffix] = SEQNUM[suffix] + 1
	else:
		SEQNUM[suffix] = 1

	return SEQNUM[suffix]

TYPES = {}
def displayTypeNum (displayType):
	global TYPES

	# alter the text to appear for DO vocab terms
	if displayType == 'DO':
		displayType = 'Disease'

	if displayType == 'OMIM':
		displayType = 'Disease'

	if not TYPES.has_key(displayType):
		TYPES[displayType] = len(TYPES) + 1

	return TYPES[displayType]

###--- Classes ---###

class AccessionGatherer:
	# Is: a data gatherer for the accession_* tables
	# Has: queries to execute against the source database
	# Does: queries the source database for accession IDs,
	#	collates results, writes tab-delimited text file

	def __init__ (self):
		return

	def buildLogicalDbFile (self):
		cmd = '''select _LogicalDB_key, name
			from acc_logicaldb
			order by _LogicalDB_key'''
		cols, rows = dbAgnostic.execute (cmd)

		path = OutputFile.createAndWrite (LDB_FILE,
			[ '_LogicalDB_key', 'name' ],
			cols, rows)
		print '%s %s' % (path, LDB_FILE)

		del cols, rows
		gc.collect()
		return

	def buildMGITypeFile (self):
		cmd = '''select _MGIType_key, case
				when name = 'Segment' then 'Probe/Clone'
				when name = 'Orthology' then 'Homology'
				when name = 'Marker Cluster' then 'Homology'
				else name
				end as name
			from acc_mgitype
			order by _MGIType_key'''
		cols, rows = dbAgnostic.execute (cmd)

		path = OutputFile.createAndWrite (OBJECT_TYPE_FILE,
			[ '_MGIType_key', 'name' ],
			cols, rows) 
		print '%s %s' % (path, OBJECT_TYPE_FILE)

		del cols, rows
		gc.collect()
		return

	def buildDisplayTypeFile (self):
		global TYPES

		cols = [ 'displayType', '_DisplayType_key' ]
		rows = TYPES.items()
		rows.sort()

		path = OutputFile.createAndWrite ('accession_display_type',
			[ '_DisplayType_key', 'displayType' ],
			cols, rows)
		print '%s %s' % (path, DISPLAY_TYPE_FILE)
		return

	def fillNonMouseMarkers (self, accessionFile):
		# only non-mouse markers, directed to HomoloGene class detail
		# page

		cmd = '''select mc.clusterID, mc._Cluster_key, o.commonName,
				m._Marker_key, a.accID, m.symbol, m.name,
				m.chromosome, a._LogicalDB_key,
				a._MGIType_key, m._Marker_Type_key
			from mrk_cluster mc,
				mrk_clustermember mcm,
				voc_term vt,
				mrk_marker m,
				mgi_organism o,
				acc_accession a
			where mc._Cluster_key = mcm._Cluster_key
				and mc._ClusterSource_key = vt._Term_key
				and vt.term = 'HomoloGene'
				and mcm._Marker_key = m._Marker_key
				and m._Organism_key = o._Organism_key
				and m._Marker_key = a._Object_key
				and a._MGIType_key = 2
				and m._Organism_key != 1
				and a._LogicalDB_key = %d''' % HGNC

		cols, rows = dbAgnostic.execute(cmd)

		clusterIdCol = dbAgnostic.columnNumber (cols, 'clusterID')
		clusterKeyCol = dbAgnostic.columnNumber (cols, '_Cluster_key')
		organismCol = dbAgnostic.columnNumber (cols, 'commonName')
		keyCol = dbAgnostic.columnNumber (cols, '_Marker_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		symbolCol = dbAgnostic.columnNumber (cols, 'symbol')
		nameCol = dbAgnostic.columnNumber (cols, 'name')
		chromosomeCol = dbAgnostic.columnNumber (cols, 'chromosome')
		ldbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
		displayTypeCol = dbAgnostic.columnNumber (cols, '_Marker_Type_key')

		outputCols = [ OutputFile.AUTO, '_Object_key', 'accID', 'displayID', 'sequenceNum',
			'description', '_LogicalDB_key', '_DisplayType_key', '_MGIType_key' ]
		outputRows = []

		for row in rows:
			accID = row[idCol]
			clusterID = row[clusterIdCol]

			displayType = Gatherer.resolve (row[displayTypeCol], 'mrk_types', '_Marker_Type_key', 'name')

			# non-mouse markers should use the fake orthology type
			organism = '(%s)' % row[organismCol]
			mgiType = ORTHOLOGY_TYPE

			# translate from non-mouse ID to its HomoloGene 
			# cluster ID
			out = [ row[keyCol], accID, clusterID, sequenceNum(clusterID),
				'%s, %s, Chr %s%s' % (row[symbolCol], row[nameCol], row[chromosomeCol], organism),
				row[ldbCol],
				displayTypeNum(displayType),
				mgiType,
				]
			outputRows.append (out) 

		logger.debug ('Found %d non-mouse marker IDs' % len(rows))

		accessionFile.writeToFile (outputCols, outputCols[1:], outputRows)
		logger.debug ('Wrote non-marker IDs to file')
		return

	def fillInterProMarkers (self, accessionFile):
	    # markers associated with InterPro IDs
		cmd1 = '''select va._Object_key, a.accID, m.symbol, m.name,
		    		m.chromosome, a._LogicalDB_key,
				t.name as typeName, a._MGIType_key
			from voc_annot va, acc_accession a, mrk_marker m,
				mrk_types t
			where a._MGIType_key = 13
				and a.private = 0
				and a._Object_key = va._Term_key
				and va._AnnotType_key = 1003
				and m._Marker_Type_key = t._Marker_Type_key
				and va._Object_key = m._Marker_key'''

		outputCols = [ OutputFile.AUTO, '_Object_key', 'accID', 'displayID', 'sequenceNum',
			'description', '_LogicalDB_key', '_DisplayType_key', '_MGIType_key' ]
		outputRows = []

		cols, rows = dbAgnostic.execute(cmd1)

		keyCol = dbAgnostic.columnNumber (cols, '_Object_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
		logicalDbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		nameCol = dbAgnostic.columnNumber (cols, 'name')
		symbolCol = dbAgnostic.columnNumber (cols, 'symbol')
		chrCol = dbAgnostic.columnNumber (cols, 'chromosome')
		typeCol = dbAgnostic.columnNumber (cols, 'typeName')

		for row in rows:
			description = '%s, %s, Chr %s' % (row[symbolCol], row[nameCol], row[typeCol])
			displayType = row[typeCol]

			outputRows.append ( [ row[keyCol], row[idCol], row[idCol], sequenceNum(row[idCol]),
				description, row[logicalDbCol], displayTypeNum(displayType), row[mgiTypeCol] ] )

		logger.debug ('Found %d markers for InterPro IDs' % len(rows))
		
		accessionFile.writeToFile (outputCols, outputCols[1:], outputRows)
		logger.debug ('Wrote markers for InterPro IDs to file')
		return

	def fillInterProTerms (self, accessionFile):
		# flat-vocab IDs, specifically InterPro
		cmd3 = '''select a._Object_key, a.accID, v.name, t.term,
	    			a._LogicalDB_key, a._MGIType_key
	    		from acc_accession a, voc_term t, voc_vocab v
			where a._MGIType_key = 13
				and a.private = 0
				and a._Object_key = t._Term_key
				and t._Vocab_key = v._Vocab_key
				and v._Vocab_key = 8'''

		outputCols = [ OutputFile.AUTO, '_Object_key', 'accID', 'displayID', 'sequenceNum',
			'description', '_LogicalDB_key', '_DisplayType_key', '_MGIType_key' ]
		outputRows = []

		cols, rows = dbAgnostic.execute(cmd3)

		keyCol = dbAgnostic.columnNumber (cols, '_Object_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
		logicalDbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		nameCol = dbAgnostic.columnNumber (cols, 'name')
		termCol = dbAgnostic.columnNumber (cols, 'term')

		for row in rows:
			description = row[termCol]
			displayType = row[nameCol]

			outputRows.append ( [ row[keyCol], row[idCol], row[idCol], sequenceNum(row[idCol]),
				description, row[logicalDbCol], displayTypeNum(displayType), row[mgiTypeCol] ] )

		logger.debug ('Found %d term IDs' % len(rows))

		accessionFile.writeToFile (outputCols, outputCols[1:], outputRows)
		logger.debug ('Wrote term IDs to file')
		return

	def main (self):
		global BASIC_ID_COUNT, SPLIT_ID_COUNT

		# build the two small files

		self.buildLogicalDbFile()
		self.buildMGITypeFile()

		# build the large file

		accessionFile = OutputFile.OutputFile (ACCESSION_FILE)
		self.fillNonMouseMarkers (accessionFile)
		self.fillInterProMarkers (accessionFile)
		self.fillInterProTerms (accessionFile)
		accessionFile.close() 

		logger.debug ('Wrote %d rows (%d columns) to %s' % (accessionFile.getRowCount(),
			accessionFile.getColumnCount(), accessionFile.getPath()) )
		print '%s %s' % (accessionFile.getPath(), ACCESSION_FILE)

		# build the third small file, which was compiled while
		# building the large file (so this one must go last)

		self.buildDisplayTypeFile()
		return

###--- main program ---###

# global instance of a AccessionGatherer
gatherer = AccessionGatherer ()

# if invoked as a script, run the gatherer's main method
if __name__ == '__main__':
	gatherer.main()
