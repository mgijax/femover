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

CHUNK_SIZE = 50000	# note that when we are chunking, we are doing the
			# same key restriction in multiple tables; while this
			# appears odd, it improves performance a lot

ORTHOLOGY_TYPE = 18

###--- Functions ---###

def splitID (accID):
	# Purpose: split the given 'accID' into a string of letters and an
	#	integer representing any digits contained
	# Notes: This should decrease the memory requirements for the script,
	#	by allowing digits to be represented in a 4-byte integer.
	#	It is used solely by the sequenceNum() function to provide an
	#	ordering of objects which have the same ID.  This compression
	#	could allow two distinct IDs (eg- "a1b2c346" and "abc12346")
	#	to share the same numbering, but the possibility of this
	#	causing a display problem is remote enough that we will ignore
	#	it for the sake of the memory savings.

	myID = accID.lower()
	letters = ''
	digits = ''

	for c in myID:
		if 'a' <= c <= 'z':
			letters = letters + c
		elif '0' <= c <= '9':
			digits = digits + c
		# skip any other characters (period, underscore, etc.)
	
	# should have more than 4 digits to see any savings (in converting
	# to a 4-byte integer)

	if len(digits) > 4:
		return letters, int(digits)
	return myID, None

# global variables for sequenceNum() function
IDS = {}		# ID -> count of objects with that ID
SPLIT_IDS = {}		# letters -> { integer number : count of objects }

SPLIT_ID_COUNT = 0
BASIC_ID_COUNT = 0

def sequenceNum (accID):
	global IDS, SPLIT_IDS, BASIC_ID_COUNT, SPLIT_ID_COUNT

	letters, digits = splitID(accID)

	# if we didn't split up the ID, just use the whole lowercase ID
	if not digits:
		if IDS.has_key (letters):
			num = IDS[letters] + 1
		else:
			num = 1
			BASIC_ID_COUNT = BASIC_ID_COUNT + 1
		IDS[letters] = num
	else:
		# first time with this letter combination?  1
		if not SPLIT_IDS.has_key(letters):
			num = 1
			SPLIT_IDS[letters] = { digits : num }
			SPLIT_ID_COUNT = SPLIT_ID_COUNT + 1

		# first time with these digits for this letter combo?  1
		elif not SPLIT_IDS[letters].has_key(digits):
			num = 1
			SPLIT_IDS[letters][digits] = num
			SPLIT_ID_COUNT = SPLIT_ID_COUNT + 1

		# already seen these letters and digits, so increment
		else:
			num = SPLIT_IDS[letters][digits] + 1
			SPLIT_IDS[letters][digits] = num
	return num

TYPES = {}
def displayTypeNum (displayType):
	global TYPES

	# alter the text to appear for OMIM vocab terms
	if displayType == 'OMIM':
		displayType = 'Disease'

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
		cmd = '''select _MGIType_key, case
				when name = 'Segment' then 'Probe/Clone'
				else name
				end as name
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
				a._MGIType_key, m._Marker_Type_key,
				m._Organism_key
			from acc_accession a, mrk_marker m
			where m._Marker_key = a._Object_key
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
		displayTypeCol = dbAgnostic.columnNumber (cols,
			'_Marker_Type_key')
		organismKey = dbAgnostic.columnNumber (cols, '_Organism_key')

		outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'displayID',
			'sequenceNum', 'description', '_LogicalDB_key',
			'_DisplayType_key', '_MGIType_key' ]
		outputRows = []

		for row in rows:
			accID = row[idCol]

			displayType = Gatherer.resolve (row[displayTypeCol],
				'mrk_types', '_Marker_Type_key', 'name')

			# non-mouse markers should use the fake orthology type
			organism = ''
			if row[organismKey] != 1:
				mgiType = ORTHOLOGY_TYPE
				organism = Gatherer.resolve (row[organismKey],
					'mgi_organism', '_Organism_key',
					'commonName')
				if organism:
					organism = '(%s)' % organism
				else:
					organism = ''
			else:
				mgiType = row[mgiTypeCol]

			out = [ row[keyCol], accID, accID, sequenceNum(accID),
				'%s, %s, Chr %s%s' % (row[symbolCol],
					row[nameCol], row[chromosomeCol],
					organism),
				row[ldbCol],
				displayTypeNum(displayType),
				mgiType,
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
			'displayID',
			'sequenceNum', 'description', '_LogicalDB_key',
			'_DisplayType_key', '_MGIType_key' ]
		outputRows = []

		for row in rows:
			accID = row[idCol]
			refsKey = row[keyCol]

			out = [ refsKey, accID, accID, sequenceNum(accID),
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
				acc._MGIType_key, a.symbol, a.name,
				a._Allele_Type_key
			from all_allele a, acc_accession acc
			where a._Allele_key = acc._Object_key
				and acc._MGIType_key = 11
				and acc.private = 0'''

	    # seq IDs for sequences associated with alleles
	    cmd2 = '''select aa._Allele_key, acc.accID,
				acc._LogicalDB_key, 11 as _MGIType_key,
				aa.symbol, aa.name, aa._Allele_Type_key
			from seq_allele_assoc saa,
				all_allele aa,
				acc_accession acc
			where acc._MGIType_key = 19
				and acc._Object_key = saa._Sequence_key
				and saa._Allele_key = aa._Allele_key'''

	    # cell line IDs for cell lines associated with alleles
	    cmd3 = '''select a._Allele_key, acc.accID, acc._LogicalDB_key,
	    			11 as _MGIType_key, a.symbol, a.name,
				a._Allele_Type_key
	    		from all_allele_cellline c, all_allele a,
				acc_accession acc
			where acc._MGIType_key = 28
				and acc._Object_key = c._MutantCellLine_key
				and c._Allele_key = a._Allele_key'''

	    queries = [ (cmd1, 'allele IDs'),
			(cmd2, 'seq IDs for alleles'),
			(cmd3, 'cell line IDs for alleles') ]

	    outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'displayID',
			'sequenceNum', 'description', '_LogicalDB_key',
			'_DisplayType_key', '_MGIType_key' ]

	    for (cmd, idType) in queries:
		cols, rows = dbAgnostic.execute(cmd)

		keyCol = dbAgnostic.columnNumber (cols, '_Allele_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		ldbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
		displayTypeCol = dbAgnostic.columnNumber (cols,
			'_Allele_Type_key')
		symbolCol = dbAgnostic.columnNumber (cols, 'symbol')
		nameCol = dbAgnostic.columnNumber (cols, 'name')

		outputRows = []
		for row in rows:
			accID = row[idCol]
			alleleKey = row[keyCol]
			description = row[symbolCol] + ', ' + row[nameCol]

			displayType = Gatherer.resolve (row[displayTypeCol])

			out = [ alleleKey, accID, accID, sequenceNum(accID),
				description, row[ldbCol],
				displayTypeNum(displayType),
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
				a._MGIType_key, p._SegmentType_key, p.name
			from prb_probe p, acc_accession a
			where p._Probe_key = a._Object_key
				and a.private = 0
				and a._MGIType_key = 3
				and p._Probe_key > %d
				and p._Probe_key <= %d
				and a._Object_key > %d
				and a._Object_key <= %d'''

	    outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'displayID',
			'sequenceNum', 'description', '_LogicalDB_key',
			'_DisplayType_key', '_MGIType_key' ]

	    while minKey < maxProbeKey:
		maxKey = minKey + CHUNK_SIZE
		cols, rows = dbAgnostic.execute(cmd % (minKey, maxKey,
			minKey, maxKey))

		keyCol = dbAgnostic.columnNumber (cols, '_Probe_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		ldbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
		displayTypeCol = dbAgnostic.columnNumber (cols,
			'_SegmentType_key')
		nameCol = dbAgnostic.columnNumber (cols, 'name')

		outputRows = []

		for row in rows:
			accID = row[idCol]
			probeKey = row[keyCol]

			displayType = Gatherer.resolve (row[displayTypeCol])

			out = [ probeKey, accID, accID, sequenceNum(accID),
				row[nameCol], row[ldbCol],
				displayTypeNum(displayType),
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
	    maxProbeKey = getMaxKey ('seq_probe_cache', '_Probe_key')

	    # sequence IDs
	    cmd1 = '''select s._Sequence_key, a.accID, a._LogicalDB_key,
	    		a._MGIType_key, s.description, s._SequenceType_key
	    	from acc_accession a, seq_sequence s
		where a._MGIType_key = 19
			and a.private = 0
			and a._Object_key = s._Sequence_key
			and a._Object_key > %d
			and a._Object_key <= %d
			and s._Sequence_key > %d
			and s._Sequence_key <= %d'''

	    # probe IDs for probes associated with sequences (from non-Genbank
	    # logical databases).  Iterate through probes, as there are fewer
	    # of them.
	    cmd2 = '''select s._Sequence_key, a.accID, a._LogicalDB_key,
	    		seqacc.accID as seqID,
			seqacc._LogicalDB_key as seqLDB,
	    		19 as _MGIType_key, s.description, s._SequenceType_key
	    	from acc_accession a, seq_probe_cache spc, seq_sequence s,
			acc_accession seqacc
		where a._MGIType_key = 3
			and a.private = 0
			and spc._Probe_key > %d
			and spc._Probe_key <= %d
			and a._Object_key > %d
			and a._Object_key <= %d
			and a._Object_key = spc._Probe_key
			and spc._Sequence_key = s._Sequence_key
			and s._Sequence_key = seqacc._Object_key
			and seqacc._MGIType_key = 19
			and seqacc.preferred = 1
			and seqacc.private = 0
			and a._LogicalDB_key != 9'''

	    queries = [ (cmd1, 'sequence IDs', maxSeqKey),
			(cmd2, 'probe IDs for sequences', maxProbeKey) ]

	    outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'displayID',
			'sequenceNum', 'description', '_LogicalDB_key',
			'_DisplayType_key', '_MGIType_key' ]

	    for (cmd, idType, maxObjectKey) in queries:
		minKey = 0
		maxKey = CHUNK_SIZE

		while minKey < maxObjectKey:
		    maxKey = minKey + CHUNK_SIZE
		    cols, rows = dbAgnostic.execute(cmd % (minKey, maxKey,
			    minKey, maxKey))

		    keyCol = dbAgnostic.columnNumber (cols, '_Sequence_key')
		    idCol = dbAgnostic.columnNumber (cols, 'accID')
		    ldbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		    mgiTypeCol = dbAgnostic.columnNumber (cols,
			'_MGIType_key')
		    displayTypeCol = dbAgnostic.columnNumber (cols,
			'_SequenceType_key')
		    descriptionCol = dbAgnostic.columnNumber (cols,
			'description')
		    if idType[:5] == 'probe':
			    seqCol = dbAgnostic.columnNumber (cols, 'seqID')
			    seqLdbCol = dbAgnostic.columnNumber (cols,'seqLDB')
		    else:
			    seqCol = None
			    seqLdbCol = None

		    # for a probe ID for a sequence, we only really want one
		    # instance per sequence, even if the same ID is associated
		    # with a probe multiple times (for multiple logical dbs)
		    idsByObject = {}

		    outputRows = []
		    for row in rows:
			accID = row[idCol]
			sequenceKey = row[keyCol]

			if idType[:5] == 'probe':
			    if idsByObject.has_key(sequenceKey):
				if accID in idsByObject[sequenceKey]:
				    continue
			    	else:
				    idsByObject[sequenceKey].append (accID)
			    else:
				    idsByObject[sequenceKey] = [ accID ]

			if seqCol:
				seqID = row[seqCol]
				ldb = row[seqLdbCol]
			else:
				seqID = accID
				ldb = row[ldbCol]

			displayType = Gatherer.resolve (row[displayTypeCol])

			out = [ sequenceKey, accID, seqID, sequenceNum(accID),
				row[descriptionCol], ldb,
				displayTypeNum(displayType),
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
			'dislayID',
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

			out = [ imageKey, accID, accID, sequenceNum(accID),
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

	def fillAntibodies (self, accessionFile):
	    cmd = '''select g._Antibody_key, a.accID, a._LogicalDB_key,
				a._MGIType_key, t.antibodyType,
				g.antibodyName
			from acc_accession a,
				gxd_antibody g,
				gxd_antibodytype t
			where a._MGIType_key = 6
				and a._Object_key = g._Antibody_key
				and g._AntibodyType_key = t._AntibodyType_key
				and a.private = 0'''

	    cols, rows = dbAgnostic.execute(cmd)

	    keyCol = dbAgnostic.columnNumber (cols, '_Antibody_key')
	    idCol = dbAgnostic.columnNumber (cols, 'accID')
	    logicalDbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
	    mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
	    typeCol = dbAgnostic.columnNumber (cols, 'antibodyType')
	    nameCol = dbAgnostic.columnNumber (cols, 'antibodyName')

	    outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'displayID', 'sequenceNum', 'description',
			'_LogicalDB_key', '_DisplayType_key', '_MGIType_key' ]
	    outputRows = []

	    for row in rows:
		    outputRows.append ( [ row[keyCol], row[idCol], row[idCol],
			sequenceNum(row[idCol]), row[nameCol],
			row[logicalDbCol], displayTypeNum(row[typeCol]),
			row[mgiTypeCol] ] )

	    logger.debug ('Found %d antibody IDs' % len(rows))

	    accessionFile.writeToFile (outputCols, outputCols[1:],
		outputRows)
	    logger.debug ('Wrote antibody IDs to file')
	    return

	def fillAntigens (self, accessionFile):
	    cmd = '''select g._Antigen_key, a.accID, a._LogicalDB_key,
	    			a._MGIType_key, g.antigenName
	    		from gxd_antigen g, acc_accession a
			where g._Antigen_key = a._Object_key
				and a.private = 0
				and a._MGIType_key = 7'''

	    cols, rows = dbAgnostic.execute(cmd)

	    keyCol = dbAgnostic.columnNumber (cols, '_Antigen_key')
	    idCol = dbAgnostic.columnNumber (cols, 'accID')
	    logicalDbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
	    mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
	    nameCol = dbAgnostic.columnNumber (cols, 'antigenName')

	    outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'displayID', 'sequenceNum', 'description',
			'_LogicalDB_key', '_DisplayType_key', '_MGIType_key' ]
	    outputRows = []

	    for row in rows:
		    outputRows.append ( [ row[keyCol], row[idCol], row[idCol],
			sequenceNum(row[idCol]), row[nameCol],
			row[logicalDbCol], displayTypeNum('Antigen'),
			row[mgiTypeCol] ] )

	    logger.debug ('Found %d antigen IDs' % len(rows))

	    accessionFile.writeToFile (outputCols, outputCols[1:],
		outputRows)
	    logger.debug ('Wrote antigen IDs to file')
	    return

	def fillAssays (self, accessionFile):
	    cmd = '''select g._Assay_key, a.accID, a._LogicalDB_key,
	    			a._MGIType_key, t.assayType, m.symbol
	    		from GXD_Assay g, ACC_Accession a, MRK_Marker m,
				GXD_AssayType t
			where exists (select 1 from GXD_Expression e where g._Assay_key = e._Assay_key)
				and a._MGIType_key = 8
				and a._Object_key = g._Assay_key
				and a.private = 0
				and g._AssayType_key = t._AssayType_key
				and g._Marker_key = m._Marker_key'''

	    cols, rows = dbAgnostic.execute(cmd)

	    keyCol = dbAgnostic.columnNumber (cols, '_Assay_key')
	    idCol = dbAgnostic.columnNumber (cols, 'accID')
	    logicalDbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
	    mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
	    typeCol = dbAgnostic.columnNumber (cols, 'assayType')
	    markerCol = dbAgnostic.columnNumber (cols, 'symbol')

	    outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'displayID', 'sequenceNum', 'description',
			'_LogicalDB_key', '_DisplayType_key', '_MGIType_key' ]
	    outputRows = []

	    for row in rows:
		    outputRows.append ( [ row[keyCol], row[idCol], row[idCol],
			sequenceNum(row[idCol]), row[markerCol],
			row[logicalDbCol], displayTypeNum(row[typeCol]),
			row[mgiTypeCol] ] )

	    logger.debug ('Found %d assay IDs' % len(rows))

	    accessionFile.writeToFile (outputCols, outputCols[1:],
		outputRows)
	    logger.debug ('Wrote assay IDs to file')
	    return

	def fillTerms (self, accessionFile):
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

	    # DAG-based IDs
	    cmd2 = '''select a._Object_key, a.accID, d.name, t.term,
	    			a._LogicalDB_key, a._MGIType_key
	    		from acc_accession a, voc_term t, dag_node n,
				dag_dag d
			where a._MGIType_key = 13
				and a.private = 0
				and a._Object_key = t._Term_key
				and t._Term_key = n._Object_key
				and n._DAG_key = d._DAG_key'''

	    # flat-vocab IDs
	    cmd3 = '''select a._Object_key, a.accID, v.name, t.term,
	    			a._LogicalDB_key, a._MGIType_key
	    		from acc_accession a, voc_term t, voc_vocab v
			where a._MGIType_key = 13
				and a.private = 0
				and a._Object_key = t._Term_key
				and t._Vocab_key = v._Vocab_key
				and not exists (select 1 from dag_node n
					where t._Term_key = n._Object_key)'''

	    outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'displayID', 'sequenceNum', 'description',
			'_LogicalDB_key', '_DisplayType_key', '_MGIType_key' ]
	    outputRows = []

	    # list of (cmd, result set type) values; types:
	    #	1 - markers associated with InterPro IDs
	    #	2 - vocab terms
	    commands = [ (cmd1, 1), (cmd2, 2), (cmd3, 2) ]

	    for (cmd, resultType) in commands:
	    	cols, rows = dbAgnostic.execute(cmd)

		keyCol = dbAgnostic.columnNumber (cols, '_Object_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
		logicalDbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		nameCol = dbAgnostic.columnNumber (cols, 'name')

		if resultType == 1:
			symbolCol = dbAgnostic.columnNumber (cols, 'symbol')
			chrCol = dbAgnostic.columnNumber (cols, 'chromosome')
			typeCol = dbAgnostic.columnNumber (cols, 'typeName')
		else:
			termCol = dbAgnostic.columnNumber (cols, 'term')

		for row in rows:
			if resultType == 1:
				description = '%s, %s, Chr %s' % (
					row[symbolCol], row[nameCol],
					row[typeCol])
				displayType = row[typeCol]
			else:
				description = row[termCol]
				displayType = row[nameCol]

			outputRows.append ( [ row[keyCol], row[idCol],
				row[idCol], sequenceNum(row[idCol]),
				description, row[logicalDbCol],
				displayTypeNum(displayType), row[mgiTypeCol]
				] )

	    logger.debug ('Found %d term IDs' % len(rows))

	    accessionFile.writeToFile (outputCols, outputCols[1:],
		outputRows)
	    logger.debug ('Wrote term IDs to file')
	    return

	def fillMapping (self, accessionFile):
	    cmd = '''select e._Expt_key, a.accID, a._LogicalDB_key,
	    			a._MGIType_key, e._Refs_key, e.exptType
	    		from mld_expts e, acc_accession a
			where a._MGIType_key = 4
				and a.private = 0
				and a._Object_key = e._Expt_key'''

	    cols, rows = dbAgnostic.execute(cmd)

	    keyCol = dbAgnostic.columnNumber (cols, '_Expt_key')
	    idCol = dbAgnostic.columnNumber (cols, 'accID')
	    logicalDbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
	    mgiTypeCol = dbAgnostic.columnNumber (cols, '_MGIType_key')
	    typeCol = dbAgnostic.columnNumber (cols, 'exptType')
	    refCol = dbAgnostic.columnNumber (cols, '_Refs_key')

	    outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'displayID', 'sequenceNum', 'description',
			'_LogicalDB_key', '_DisplayType_key', '_MGIType_key' ]
	    outputRows = []

	    for row in rows:
		    description = ReferenceCitations.getMiniCitation (
			row[refCol])

		    outputRows.append ( [ row[keyCol], row[idCol], row[idCol],
			sequenceNum(row[idCol]), description,
			row[logicalDbCol], displayTypeNum(row[typeCol]),
			row[mgiTypeCol] ] )

	    logger.debug ('Found %d mapping IDs' % len(rows))

	    accessionFile.writeToFile (outputCols, outputCols[1:],
		outputRows)
	    logger.debug ('Wrote mapping IDs to file')
	    return

	def fillConsensusSnps (self, accessionFile):
	    maxSnpKey = getMaxKey ('snp_consensussnp', '_ConsensusSnp_key')

	    cmd = '''select s._ConsensusSnp_key, a.accID, a._LogicalDB_key,
	    			s.alleleSummary, s._VarClass_key
			from snp_accession a, snp_consensussnp s
			where a._MGIType_key = 30
				and a._Object_key = s._ConsensusSnp_key
				and s._ConsensusSnp_key > %d
				and s._ConsensusSnp_key <= %d
				and a._Object_key > %d
				and a._Object_key <= %d'''

	    outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'displayID', 'sequenceNum', 'description',
			'_LogicalDB_key', '_DisplayType_key', '_MGIType_key' ]

	    minKey = 0
	    maxKey = CHUNK_SIZE

	    while minKey < maxSnpKey:
		maxKey = minKey + CHUNK_SIZE
		cols, rows = dbAgnostic.execute(cmd % (minKey, maxKey,
			minKey, maxKey))

		keyCol = dbAgnostic.columnNumber (cols, '_ConsensusSnp_key')
		idCol = dbAgnostic.columnNumber (cols, 'accID')
		ldbCol = dbAgnostic.columnNumber (cols, '_LogicalDB_key')
		typeCol = dbAgnostic.columnNumber (cols, '_VarClass_key')
		summaryCol = dbAgnostic.columnNumber (cols, 'alleleSummary')

		outputRows = []

		for row in rows:
			accID = row[idCol]
			snpKey = row[keyCol]

			displayType = Gatherer.resolve (row[typeCol])

			out = [ snpKey, accID, accID, sequenceNum(accID),
				row[summaryCol], row[ldbCol],
				displayTypeNum(displayType),
				30,
				]
			outputRows.append (out) 

		logger.debug ('Found %d SNP IDs (%d-%d)' % (len(rows),
			minKey, maxKey))

		accessionFile.writeToFile (outputCols, outputCols[1:],
			outputRows)
		logger.debug ('Wrote SNP IDs to file')

	    	minKey = maxKey
	    return

	def fillSubSnps (self, accessionFile):
	    maxSubSnpKey = getMaxKey ('snp_subsnp', '_SubSnp_key')

	    cmd = '''select s._ConsensusSnp_key,
	    			a.accID as subsnpID,
				a._LogicalDB_key as subsnpLDB,
	    			s._VarClass_key as subsnpType,
	    			a2.accID as snpID,
				a2._LogicalDB_key as snpLDB
			from snp_accession a,
				snp_accession a2,
				snp_subsnp s
			where a._MGIType_key = 31
				and a._Object_key = s._SubSnp_key
				and s._SubSnp_key > %d
				and s._SubSnp_key <= %d
				and a._Object_key > %d
				and a._Object_key <= %d
				and a2._MGIType_key = 30
				and a2._Object_key = s._ConsensusSnp_key'''

	    outputCols = [ OutputFile.AUTO, '_Object_key', 'accID',
			'displayID', 'sequenceNum', 'description',
			'_LogicalDB_key', '_DisplayType_key', '_MGIType_key' ]

	    minKey = 0
	    maxKey = CHUNK_SIZE

	    while minKey < maxSubSnpKey:
		maxKey = minKey + CHUNK_SIZE
		cols, rows = dbAgnostic.execute(cmd % (minKey, maxKey,
			minKey, maxKey))

		keyCol = dbAgnostic.columnNumber (cols, '_ConsensusSnp_key')
		subsnpIDCol = dbAgnostic.columnNumber (cols, 'subsnpID')
		snpIDCol = dbAgnostic.columnNumber (cols, 'snpID')
		subsnpLdbCol = dbAgnostic.columnNumber (cols, 'subsnpLDB')
		snpLdbCol = dbAgnostic.columnNumber (cols, 'snpLDB')
		subsnpTypeCol = dbAgnostic.columnNumber (cols, 'subsnpType')

		outputRows = []

		for row in rows:
			subsnpID = row[subsnpIDCol]
			snpID = row[snpIDCol]
			snpKey = row[keyCol]

			displayType = Gatherer.resolve (row[subsnpTypeCol])

			out = [ snpKey, subsnpID, subsnpID,
				sequenceNum(subsnpID),
				'submitted SNP for %s' % snpID,
				row[subsnpLdbCol],
				displayTypeNum(displayType),
				30,
				]
			outputRows.append (out) 

		logger.debug ('Found %d SubSNP IDs (%d-%d)' % (len(rows),
			minKey, maxKey))

		accessionFile.writeToFile (outputCols, outputCols[1:],
			outputRows)
		logger.debug ('Wrote SubSNP IDs to file')

	    	minKey = maxKey
	    return

	def main (self):
		global BASIC_ID_COUNT, SPLIT_ID_COUNT

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
		self.fillAssays (accessionFile)
		self.fillAntibodies (accessionFile)
		self.fillAntigens (accessionFile)
		self.fillTerms (accessionFile)
		self.fillMapping (accessionFile)
		self.fillConsensusSnps (accessionFile)
		self.fillSubSnps (accessionFile)

		accessionFile.close() 
		logger.debug ('Wrote %d rows (%d columns) to %s' % (
			accessionFile.getRowCount(),
			accessionFile.getColumnCount(),
			accessionFile.getPath()) )
		print '%s %s' % (accessionFile.getPath(), ACCESSION_FILE)

		logger.debug ('%d split IDs, %d not split' % (
			SPLIT_ID_COUNT, BASIC_ID_COUNT))

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
