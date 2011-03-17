#!/usr/local/bin/python
# 
# gathers data for the accession_* tables in the front-end database:
#	accession, accession_logical_db, accession_display_type,
#	accession_object_type

# This gatherer is more custom than the other Gatherers, because of the size
# of the data set.  It would be good to use a MultiFileGatherer because we are
# producing multiple tables, but it would also be good to use a ChunkGatherer
# because of the size of the data set for the main "accession" table.  So,
# this will be an entirely custom Gatherer, drawing ideas from them both.

import Gatherer
import logger
import OutputFile
import dbAgnostic
import ReferenceCitations

###--- Globals ---###

Error = Gatherer.Error
mismatchError = 'Mismatch in %s and %s columns'

LDB_FILE = 'accession_logical_db'
OBJECT_TYPE_FILE = 'accession_object_type'
ACCESSION_FILE = 'accession'
DISPLAY_TYPE_FILE = 'accession_display_type'

CHUNK_SIZE = 300000

###--- Functions ---###

IDS = {}		# ID -> count of objects with that ID
def sequenceNum (accID):
	global IDS

	if IDS.has_key (accID):
		num = IDS[accID] + 1
	else:
		num = 1

	IDS[accID] = num
	return num

TYPES = {}
def displayTypeNum (displayType):
	global TYPES

	if not TYPES.has_key(displayType):
		TYPES[displayType] = len(TYPES) + 1

	return TYPES[displayType]

def getMaxKey (tableName, fieldName):
	cmd = 'select max(%s) as maxCount from %s' % (fieldName, tableName)

	cols, rows = dbAgnostic.execute(cmd)
	if not rows:
		return 0

	countCol = dbAgnostic.columnNumber (cols, 'maxCount')
	count = rows[0][countCol] 

	logger.debug ('max(%s.%s) = %d' % (tableName, fieldName, count))
	return count

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
		return

	def buildMGITypeFile (self):
		cmd = '''select _MGIType_key, name
			from acc_mgitype
			order by _MGIType_key'''
		cols, rows = dbAgnostic.execute (cmd)

		path = OutputFile.createAndWrite (OBJECT_TYPE_FILE,
			[ '_MGIType_key', 'name' ],
			cols, rows) 
		print '%s %s' % (path, OBJECT_TYPE_FILE)
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

	def fillMarkers (self, accessionFile):
		# both mouse and non-mouse markers
		cmd = '''select m._Marker_key, a.accID, m.symbol, m.name,
				m.chromosome, a._LogicalDB_key,
				a._MGIType_key, t.name as typeName
			from acc_accession a, mrk_marker m, mrk_types t
			where m._Marker_key = a._Object_key
				and m._Marker_Type_key = t._Marker_Type_key
				and a._MGIType_key = 2
				and a.private = 0'''

		cols, rows = dbAgnostic.execute(cmd)

		keyCol = dbAgnostic.columnNumber (cols, '_Marker_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		symbolCol = dbAgnostic.columnNumber (cols, 'symbol')
		nameCol = dbAgnostic.columnNumber (cols, 'name')
		chromosomeCol = dbAgnostic.columnNumber (cols, 'chromosome')
		ldbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
		displayTypeCol = dbAgnostic.columnNumber (cols, 'typeName')

		outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'sequenceNum', 'description', '_LogicalDB_key',
			'_DisplayType_key', '_MGIType_key' ]
		outputRows = []

		for row in rows:
			accID = row[idCol]

			out = [ row[keyCol], accID, sequenceNum(accID),
				'%s, %s, Chr %s' % (row[symbolCol],
					row[nameCol], row[chromosomeCol]),
				row[ldbCol],
				displayTypeNum(row[displayTypeCol]),
				row[mgiTypeCol],
				]
			outputRows.append (out) 

		logger.debug ('Found %d marker IDs' % len(rows))

		accessionFile.writeToFile (outputCols, outputCols[1:],
			outputRows)
		logger.debug ('Wrote marker IDs to file')
		return

	def fillReferences (self, accessionFile):
		cmd = '''select c._Refs_key, a.accID, a._LogicalDB_key,
				a._MGIType_key, 'Reference' as typeName
			from bib_citation_cache c, acc_accession a
			where c._Refs_key = a._Object_key
				and a._MGIType_key = 1
				and a.private = 0
				and (c.journal != 'Submission'
					or c.journal is null)'''

		cols, rows = dbAgnostic.execute(cmd)

		keyCol = dbAgnostic.columnNumber (cols, '_Refs_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		ldbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
		displayTypeCol = dbAgnostic.columnNumber (cols, 'typeName')

		outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'sequenceNum', 'description', '_LogicalDB_key',
			'_DisplayType_key', '_MGIType_key' ]
		outputRows = []

		for row in rows:
			accID = row[idCol]
			refsKey = row[keyCol]

			out = [ refsKey, accID, sequenceNum(accID),
				ReferenceCitations.getMiniCitation(refsKey),
				row[ldbCol],
				displayTypeNum(row[displayTypeCol]),
				row[mgiTypeCol],
				]
			outputRows.append (out) 

		logger.debug ('Found %d reference IDs' % len(rows))

		accessionFile.writeToFile (outputCols, outputCols[1:],
			outputRows)
		logger.debug ('Wrote reference IDs to file')
		return

	def fillAlleles (self, accessionFile):

	    # IDs for alleles
	    cmd1 = '''select a._Allele_key, acc.accID, acc._LogicalDB_key,
				acc._MGIType_key, t.term, a.symbol, a.name
			from all_allele a, voc_term t, acc_accession acc
			where a._Allele_key = acc._Object_key
				and acc._MGIType_key = 11
				and acc.private = 0
				and a._Allele_Type_key = t._Term_key'''

	    # seq IDs for sequences associated with alleles
	    cmd2 = '''select aa._Allele_key, acc.accID,
				acc._LogicalDB_key, 11 as _MGIType_key,
				t.term, aa.symbol, aa.name
			from seq_allele_assoc saa,
				all_allele aa,
				acc_accession acc,
				voc_term t
			where acc._MGIType_key = 19
				and acc._Object_key = saa._Sequence_key
				and saa._Allele_key = aa._Allele_key
				and aa._Allele_Type_key = t._Term_key'''

	    # cell line IDs for cell lines associated with alleles
	    cmd3 = '''select a._Allele_key, acc.accID, acc._LogicalDB_key,
	    			11 as _MGIType_key, t.term, a.symbol, a.name
	    		from all_allele_cellline c, all_allele a,
				acc_accession acc, voc_term t
			where acc._MGIType_key = 28
				and acc._Object_key = c._MutantCellLine_key
				and c._Allele_key = a._Allele_key
				and a._Allele_Type_key = t._Term_key'''

	    queries = [ (cmd1, 'allele IDs'),
			(cmd2, 'seq IDs for alleles'),
			(cmd3, 'cell line IDs for alleles') ]

	    outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'sequenceNum', 'description', '_LogicalDB_key',
			'_DisplayType_key', '_MGIType_key' ]

	    for (cmd, idType) in queries:
		cols, rows = dbAgnostic.execute(cmd)

		keyCol = dbAgnostic.columnNumber (cols, '_Allele_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		ldbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
		displayTypeCol = dbAgnostic.columnNumber (cols, 'term')
		symbolCol = dbAgnostic.columnNumber (cols, 'symbol')
		nameCol = dbAgnostic.columnNumber (cols, 'name')

		outputRows = []
		for row in rows:
			accID = row[idCol]
			alleleKey = row[keyCol]
			description = row[symbolCol] + ', ' + row[nameCol]

			out = [ alleleKey, accID, sequenceNum(accID),
				description, row[ldbCol],
				displayTypeNum(row[displayTypeCol]),
				row[mgiTypeCol],
				]
			outputRows.append (out) 

		logger.debug ('Found %d %s' % (len(rows), idType))

		accessionFile.writeToFile (outputCols, outputCols[1:],
			outputRows)
		logger.debug ('Wrote %s to file' % idType)
	    return

	def fillProbes (self, accessionFile):
	    # large data set, so walk through it in chunks

	    maxProbeKey = getMaxKey ('prb_probe', '_Probe_key')

	    minKey = 0
	    maxKey = CHUNK_SIZE

	    cmd = '''select p._Probe_key, a.accID, a._LogicalDB_key,
				a._MGIType_key, t.term, p.name
			from prb_probe p, voc_term t, acc_accession a
			where p._Probe_key = a._Object_key
				and a.private = 0
				and a._MGIType_key = 3
				and p._Probe_key > %d
				and p._Probe_key <= %d
				and p._SegmentType_key = t._Term_key'''

	    outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'sequenceNum', 'description', '_LogicalDB_key',
			'_DisplayType_key', '_MGIType_key' ]

	    while minKey < maxProbeKey:
		maxKey = minKey + CHUNK_SIZE
		cols, rows = dbAgnostic.execute(cmd % (minKey, maxKey))

		keyCol = dbAgnostic.columnNumber (cols, '_Probe_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		ldbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
		displayTypeCol = dbAgnostic.columnNumber (cols, 'term')
		nameCol = dbAgnostic.columnNumber (cols, 'name')

		outputRows = []

		for row in rows:
			accID = row[idCol]
			probeKey = row[keyCol]

			out = [ probeKey, accID, sequenceNum(accID),
				row[nameCol], row[ldbCol],
				displayTypeNum(row[displayTypeCol]),
				row[mgiTypeCol],
				]
			outputRows.append (out) 

		logger.debug ('Found %d probe IDs (%d-%d)' % (len(rows),
			minKey, maxKey))

		accessionFile.writeToFile (outputCols, outputCols[1:],
			outputRows)
		logger.debug ('Wrote probe IDs to file')

	    	minKey = maxKey

	    return

	def fillSequences (self, accessionFile):

	    maxSeqKey = getMaxKey ('seq_sequence', '_Sequence_key')

	    # sequence IDs
	    cmd1 = '''select s._Sequence_key, a.accID, a._LogicalDB_key,
	    		a._MGIType_key, t.term, s.description
	    	from acc_accession a, seq_sequence s, voc_term t
		where a._MGIType_key = 19
			and a.private = 0
			and a._Object_key = s._Sequence_key
			and a._Object_key > %d
			and a._Object_key <= %d
			and s._SequenceType_key = t._Term_key'''

	    # probe IDs for probes associated with sequences (from non-Genbank
	    # logical databases)
	    cmd2 = '''select s._Sequence_key, a.accID, a._LogicalDB_key,
	    		19 as _MGIType_key, t.term, s.description
	    	from acc_accession a, seq_probe_cache spc, seq_sequence s,
			voc_term t
		where a._MGIType_key = 3
			and a.private = 0
			and s._Sequence_key > %d
			and s._Sequence_key <= %d
			and a._Object_key = spc._Probe_key
			and spc._Sequence_key = s._Sequence_key
			and s._SequenceType_key = t._Term_key
			and a._LogicalDB_key != 9'''

	    queries = [ (cmd1, 'sequence IDs'),
			(cmd2, 'probe IDs for sequences') ]

	    outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'sequenceNum', 'description', '_LogicalDB_key',
			'_DisplayType_key', '_MGIType_key' ]

	    for (cmd, idType) in queries:
		minKey = 0
		maxKey = CHUNK_SIZE

		while minKey < maxSeqKey:
		    maxKey = minKey + CHUNK_SIZE
		    cols, rows = dbAgnostic.execute(cmd % (minKey, maxKey))

		    keyCol = dbAgnostic.columnNumber (cols, '_Sequence_key')
		    idCol = dbAgnostic.columnNumber (cols, 'accID')
		    ldbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		    mgiTypeCol = dbAgnostic.columnNumber (cols,
			'_MGIType_key')
		    displayTypeCol = dbAgnostic.columnNumber (cols,
			'term')
		    descriptionCol = dbAgnostic.columnNumber (cols,
			'description')

		    outputRows = []
		    for row in rows:
			accID = row[idCol]
			sequenceKey = row[keyCol]

			out = [ sequenceKey, accID, sequenceNum(accID),
				row[descriptionCol], row[ldbCol],
				displayTypeNum(row[displayTypeCol]),
				row[mgiTypeCol],
				]
			outputRows.append (out) 

		    logger.debug ('Found %d %s (%d-%d)' % (len(rows), idType,
			minKey, maxKey))

		    accessionFile.writeToFile (outputCols, outputCols[1:],
			outputRows)
		    logger.debug ('Wrote %s to file' % idType)

		    minKey = maxKey
	    return

	def fillImages (self, accessionFile):
		cmd = '''select i._Image_key, a.accID, a._LogicalDB_key,
				a._MGIType_key, 'Image' as term,
				i.figureLabel, r._primary, r.journal,
				r.date, r.vol, r.issue, r.pgs
			from img_image i, bib_refs r, acc_accession a
			where a._Object_key = i._Image_key
				and a.private = 0
				and a._MGIType_key = 9
				and i._Refs_key = r._Refs_key'''

		cols, rows = dbAgnostic.execute(cmd)

		keyCol = dbAgnostic.columnNumber (cols, '_Image_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		ldbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
		displayTypeCol = dbAgnostic.columnNumber (cols, 'term')
		labelCol = dbAgnostic.columnNumber (cols, 'figureLabel')
		authorCol = dbAgnostic.columnNumber (cols, '_primary')
		journalCol = dbAgnostic.columnNumber (cols, 'journal')
		dateCol = dbAgnostic.columnNumber (cols, 'date')
		volCol = dbAgnostic.columnNumber (cols, 'vol')
		issueCol = dbAgnostic.columnNumber (cols, 'issue')
		pgsCol = dbAgnostic.columnNumber (cols, 'pgs')

		outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'sequenceNum', 'description', '_LogicalDB_key',
			'_DisplayType_key', '_MGIType_key' ]
		outputRows = []

		for row in rows:
			accID = row[idCol]
			imageKey = row[keyCol]

			description = '%s %s, %s %s;%s(%s):%s' % (
				row[labelCol], row[authorCol],
				row[journalCol], row[dateCol], row[volCol],
				row[issueCol], row[pgsCol])

			out = [ imageKey, accID, sequenceNum(accID),
				description, row[ldbCol],
				displayTypeNum(row[displayTypeCol]),
				row[mgiTypeCol],
				]
			outputRows.append (out) 

		logger.debug ('Found %d image IDs' % len(rows))

		accessionFile.writeToFile (outputCols, outputCols[1:],
			outputRows)
		logger.debug ('Wrote image IDs to file')
		return

	def main (self):

		# build the two small files

		self.buildLogicalDbFile()
		self.buildMGITypeFile()

		# build the large file

		accessionFile = OutputFile.OutputFile (ACCESSION_FILE)
		self.fillMarkers (accessionFile)
		self.fillReferences (accessionFile)
		self.fillAlleles (accessionFile)
		self.fillProbes (accessionFile)
		self.fillSequences (accessionFile)
		self.fillImages (accessionFile)

		accessionFile.close() 
		logger.debug ('Wrote %d rows (%d columns) to %s' % (
			accessionFile.getRowCount(),
			accessionFile.getColumnCount(),
			accessionFile.getPath()) )
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
