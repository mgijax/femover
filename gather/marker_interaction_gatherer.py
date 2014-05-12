#!/usr/local/bin/python
# 
# gathers data for the 'marker_interaction' and 'marker_interaction_property'
# tables in the front-end database.  This data set is large with the potential
# to grow very large, so memory usage is at a premium.  As a result, this
# gather is completely customized (not using a Gatherer subclass).

import sys
import copy
import KeyGenerator
import Gatherer
import logger
import types
import dbAgnostic
import random
import MarkerUtils 
import GroupedList
import gc
import ReferenceUtils
import VocabUtils
import OutputFile
import symbolsort

###--- Globals ---###

interactionKey = 1001	# _Category_key for interactions

maxRowCount = 100000	# max number of interaction rows to process in memory

rowsPerMarker = {}	# maps marker key -> count of interaction rows with
			# ...this marker as organizer

maxMarkerKey = None	# maximum (organizer) marker key to process

categoryName = {}	# category key -> category name

notePropertyName = 'note'	# name of property for relationship notes

interactionRowCount = 0	# count of rows for interactions

allMarkers = 0		# count of all markers which are organizers

teasers = None		# dictionary of marker keys, referring to a list of
			# marker keys for its teaser markers

# generator for mi_key values, based on relationship key & reversed flag

miGenerator = KeyGenerator.KeyGenerator('marker_interaction')

# output files

interactionFile = OutputFile.OutputFile ('marker_interaction')
propertyFile = OutputFile.OutputFile ('marker_interaction_property')

###--- Functions ---###

def initialize():
	# set up global variables needed for processing

	global maxMarkerKey, rowsPerMarker, allMarkers, teasers

	# find maximum marker key with interactions

	cmd2 = '''select max(_Object_key_1)
		from mgi_relationship
		where _Category_key = %d''' % interactionKey

	(cols, rows) = dbAgnostic.execute(cmd2)
	maxMarkerKey = rows[0][0]
	logger.debug('Max marker key w/ interactions: %d' % maxMarkerKey)

	# find number of interactions per marker, where the marker is the
	# organizer

	cmd3 = '''select _Object_key_1, count(1) as ct
		from mgi_relationship
		where _Category_key = %d
		group by _Object_key_1''' % interactionKey

	(cols, rows) = dbAgnostic.execute(cmd3)
	keyCol = dbAgnostic.columnNumber (cols, '_Object_key_1')
	ctCol = dbAgnostic.columnNumber (cols, 'ct')

	for row in rows:
		rowsPerMarker[row[keyCol]] = row[ctCol]

	allMarkers = len(rowsPerMarker)

	logger.debug('Got interaction counts for %d markers' % allMarkers)

	# look up the various category names and cache them by key

	cmd4 = 'select _Category_key, name from mgi_relationship_category'

	(cols, rows) = dbAgnostic.execute(cmd4)
	keyCol = dbAgnostic.columnNumber(cols, '_Category_key')
	nameCol = dbAgnostic.columnNumber(cols, 'name')

	for row in rows:
		categoryName[row[keyCol]] = row[nameCol]
	
	logger.debug('Got %d category names' % len(categoryName))

	# restrict our set of cached J: numbers to only those cited in the
	# relationship table

	ReferenceUtils.restrict('mgi_relationship')

	# get the teaser markers
	teasers = getTeaserMarkers()
	return

def getNextMarkerKey(markerKey):
	# get the next marker key higher than the given 'markerKey' which
	# is the organizer for interactions.  Returns None if there are no
	# higher marker keys that have interactions.

	k = markerKey + 1
	while not rowsPerMarker.has_key(k):
		k = k + 1

		if k > maxMarkerKey:
			return None
	return k

def getMarkerRange(previousMax = 0):
	# get a (start marker key, end marker key, number of markers) triple,
	# identifying the next marker range to process, where the start marker
	# key will be the next marker with interactions beyond the given
	# previousMax marker key. Returns (None, None, 0) if there are no more
	# markers to process.

	currentMarkers = 0

	startMarker = getNextMarkerKey(previousMax)
	if startMarker == None:
		return None, None, 0

	endMarker = startMarker		# assume only one marker fits

	currentMarkers = 1

	soFar = rowsPerMarker[startMarker]

	nextMarker = getNextMarkerKey(startMarker)

	if nextMarker == None:
		return startMarker, startMarker, currentMarkers

	nextCount = rowsPerMarker[nextMarker]

	while (soFar + nextCount <= maxRowCount):
		currentMarkers = currentMarkers + 1

		soFar = soFar + nextCount
		endMarker = nextMarker

		nextMarker = getNextMarkerKey(nextMarker)
		if nextMarker != None:
			nextCount = rowsPerMarker[nextMarker]
		else:
			break

	return startMarker, endMarker, currentMarkers

def getTeaserMarkers():
	# get a dictionary where each marker key refers to a list of its
	# teaser markers (up to three)
	# Returns: { marker key : [ teaser marker 1, ... teaser marker 3 ] }

	# to keep memory requirements down, we will go through the set of
	# records in chunks by marker key. 

	# marker key : list of up to three teaser marker keys (to be returned)
	teasers = {}

	# We already have the global rowsPerMarker identifying the number of
	# rows where each marker is an organizer.  We now need to pull out the
	# number of rows where they are participants and add them, to get the
	# total number of rows we'll need to process for each marker.

	cmd1 = '''select _Object_key_2, count(1) as ct
		from mgi_relationship
		where _Category_key = %d
		group by _Object_key_2''' % interactionKey

	(cols, rows) = dbAgnostic.execute(cmd1)
	keyCol = dbAgnostic.columnNumber (cols, '_Object_key_2')
	ctCol = dbAgnostic.columnNumber (cols, 'ct')

	# marker key -> number of rows involving it (start with organizer rows
	# then add in the participant rows)
	totalRows = copy.copy(rowsPerMarker)

	for row in rows:
		key = row[keyCol]
		ct = row[ctCol]

		if totalRows.has_key(key):
			totalRows[key] = totalRows[key] + ct
		else:
			# marker is only a participant, never an organizer
			totalRows[key] = ct

	markerKeys = totalRows.keys()
	markerKeys.sort()

	del cols
	del rows
	gc.collect()

	logger.debug('Need to find teasers for %d markers from %d to %d' % (
		len(markerKeys), markerKeys[0], markerKeys[-1]))

	# let's create our groups of markers, one group for each execution of
	# the query

	maxMarkersPerGroup = 20
	groups = []
	group = []
	groupRows = 0
	groupMarkers = 0

	for markerKey in markerKeys:
		rowCount = totalRows[markerKey]

		# if we can fit this marker's rows into our existing group,
		# then do it

		if groupRows + rowCount <= maxRowCount:
			group.append(markerKey)
			groupRows = groupRows + rowCount

			# if adding this marker made our group hit its maximum
			# number of markers, save this group and start a new
			# one

			if len(group) == maxMarkersPerGroup:
				groups.append(group)
				group = []
				groupRows = 0

		# special case - if just this marker is more than the number
		# of rows per group, then add a special group just for this
		# one and continue on with our current group.

		elif rowCount > maxRowCount:
			groups.append ( [ markerKey ] )

		# otherwise, we need to save the current group and start a new
		# one with the new marker

		else:
			groups.append(group)

			group = [ markerKey ]
			groupRows = rowCount

	# if we (as is likely) end with a non-empty group, then add it to our
	# set of groups, too.

	if group:
		groups.append(group)

	# Now, we have our groups of markers broken down into bite-size chunks
	# we can use in queries.

	for group in groups:
		markers = ','.join(map(str, group))

		# marker key : { related marker key : 1 }
		relatedMarkers = {}

		# gather the related markers where the ones in our group are
		# the organizers

		cmd1 = '''select _Object_key_1, _Object_key_2
			from mgi_relationship
			where _Category_key = %d
			and _Object_key_1 in (%s)''' % (interactionKey,
				markers)

		(cols1, rows1) = dbAgnostic.execute(cmd1)

		organizerCol = dbAgnostic.columnNumber(cols1, '_Object_key_1')
		participantCol = dbAgnostic.columnNumber(cols1, '_Object_key_2')

		for row in rows1:
			organizer = row[organizerCol]
			participant = row[participantCol]


			# skip self-interacting markers
			if organizer == participant:
				continue

			if not relatedMarkers.has_key(organizer):
				relatedMarkers[organizer] = {}
			relatedMarkers[organizer][participant] = 1

		del cols1
		del rows1
		gc.collect()

		# gather the related markers where the ones in our group are
		# the participants

		cmd2 = '''select distinct _Object_key_1, _Object_key_2
			from mgi_relationship
			where _Category_key = %d
			and _Object_key_2 in (%s)''' % (interactionKey,
				markers)

		(cols2, rows2) = dbAgnostic.execute(cmd2)

		organizerCol = dbAgnostic.columnNumber(cols2, '_Object_key_1')
		participantCol = dbAgnostic.columnNumber(cols2, '_Object_key_2')

		for row in rows2:
			organizer = row[organizerCol]
			participant = row[participantCol]


			# skip self-interacting markers
			if organizer == participant:
				continue

			if not relatedMarkers.has_key(participant):
				relatedMarkers[participant] = {}
			relatedMarkers[participant][organizer] = 1

		del cols2
		del rows2
		gc.collect()

		# now for each marker, we need to sort its related markers and
		# identify the three that will be its teasers.

		for marker in group:
			markerList = []

			for relatedMarker in relatedMarkers[marker]:
				markerList.append ( (
					MarkerUtils.getSymbol(marker),
					marker) )

			markerList.sort (lambda x, y : symbolsort.nomenCompare(
				x[0], y[0]) )

			teasers[marker] = []
			for (symbol, key) in markerList[:3]:
				teasers[marker].append(key)

		del relatedMarkers
		del markerList
		gc.collect()

	del groups
	gc.collect()

	logger.debug('Returning teasers for %d markers' % len(teasers))
	return teasers

def getInteractionRows(startMarker, endMarker):
	# get the basic interaction rows for organizer markers with keys
	# between 'startMarker' and 'endMarker'

	cmd = '''select _Relationship_key,
			_Object_key_1 as marker_key,
			_Object_key_2 as interacting_marker_key,
			_Refs_key,
			_RelationshipTerm_key,
			_Qualifier_key,
			_Evidence_key
		from mgi_relationship
		where _Category_key = %d
			and _Object_key_1 >= %d
			and _Object_key_1 <= %d
		order by _Object_key_1''' % (interactionKey, startMarker,
			endMarker)

	(cols, rows) = dbAgnostic.execute(cmd)

	logger.debug ('Got %d interactions for markers %d-%d' % (
		len(rows), startMarker, endMarker))
	return cols, rows

def getPropertyRows(startMarker, endMarker):
	# get the property rows for organizer markers with keys between
	# 'startMarker' and 'endMarker'

	# properties for relationships in our marker range

	cmd1 = '''select p._Relationship_key,
			p._PropertyName_key,
			p.value,
			p.sequenceNum
		from mgi_relationship_property p,
			mgi_relationship r
		where p._Relationship_key = r._Relationship_key
			and r._Category_key = %d
			and r._Object_key_1 >= %d
			and r._Object_key_1 <= %d
		order by p._Relationship_key, p.sequenceNum''' % (
			interactionKey, startMarker, endMarker)

	(cols1, rows1) = dbAgnostic.execute(cmd1)

	logger.debug('Got %d properties for markers %d-%d' % (
		len(rows1), startMarker, endMarker))

	# notes for relationships in our marker range (needed for Excel/tab
	# downloads)

	cmd2 = '''select r._Relationship_key,
			c._Note_key,
			c.sequenceNum,
			c.note
		from mgi_relationship r,
			mgi_notetype t,
			mgi_note n,
			mgi_notechunk c
		where r._Relationship_key = n._Object_key
			and r._Category_key = %d
			and t._NoteType_key = n._NoteType_key
			and t._MGIType_key = 40
			and n._Note_key = c._Note_key
			and r._Object_key_1 >= %d
			and r._Object_key_1 <= %d
		order by r._Relationship_key, c._Note_key, c.sequenceNum''' % (
			interactionKey, startMarker, endMarker)

	(cols2, rows2) = dbAgnostic.execute(cmd2)

	keyCol = dbAgnostic.columnNumber (cols2, '_Relationship_key')
	noteCol = dbAgnostic.columnNumber (cols2, 'note')
	noteKeyCol = dbAgnostic.columnNumber (cols2, '_Note_key')

	notes = {}	# relationship key -> string note

	prevNoteKey = None

	for row in rows2:
		relationshipKey = row[keyCol]
		noteKey = row[noteKeyCol]

		if not notes.has_key(relationshipKey):
			notes[relationshipKey] = row[noteCol]

		elif noteKey == prevNoteKey:
			notes[relationshipKey] = notes[relationshipKey] + \
				row[noteCol]

		else:
			notes[relationshipKey] = notes[relationshipKey] + \
				'; ' + row[noteCol]

		prevNoteKey = noteKey

#	logger.debug('Collated %d rows into %d notes for markers %d-%d' % (
#		len(rows2), len(notes), startMarker, endMarker))

	# add the notes to the list of other properties

	keyCol = dbAgnostic.columnNumber (cols1, '_Relationship_key')
	nameCol = dbAgnostic.columnNumber (cols1, '_PropertyName_key')
	valueCol = dbAgnostic.columnNumber (cols1, 'value')
	seqNumCol = dbAgnostic.columnNumber (cols1, 'sequenceNum') 

	fields = [ 0, 1, 2, 3 ]
	i = len(rows1)

	for relationshipKey in notes.keys():
		row = []
		for field in fields:
			if field == keyCol:
				row.append(relationshipKey)
			elif field == nameCol:
				row.append(notePropertyName)
			elif field == valueCol:
				row.append(notes[relationshipKey])
			elif field == seqNumCol:
				i = i + 1
				row.append(i)
		rows1.append(row)

	logger.debug('Added %d notes as properties for markers %d-%d' % (
		len(notes), startMarker, endMarker))

	del cols2
	del rows2
	del notes
	gc.collect()

	return cols1, rows1

def expandInteractionRows(iCols, iRows, reverse = 0):
	# produce the interaction rows for the data file, given the 'iCols'
	# and 'iRows' as retrieved from getInteractionRows().  If 'reverse' is
	# 0, then we produce organizer-to-participant rows.  If it is 1, then
	# do produce participant-to-organizer rows.

	global interactionRowCount

	cols = [ 'mi_key', 'marker_key', 'interacting_marker_key',
		'interacting_marker_symbol', 'interacting_marker_id',
		'relationship_category', 'relationship_term',
		'qualifier', 'evidence_code', 'reference_key',
		'jnum_id', 'sequence_num', 'in_teaser', 'is_reversed' ]
	rows = []

	relationshipCol = dbAgnostic.columnNumber(iCols, '_Relationship_key')
	organizerCol = dbAgnostic.columnNumber(iCols, 'marker_key')
	participantCol = dbAgnostic.columnNumber(iCols,
		'interacting_marker_key')
	refsCol = dbAgnostic.columnNumber(iCols, '_Refs_key')
	termCol = dbAgnostic.columnNumber(iCols, '_RelationshipTerm_key')
	qualifierCol = dbAgnostic.columnNumber(iCols, '_Qualifier_key')
	evidenceCol = dbAgnostic.columnNumber(iCols, '_Evidence_key')

	category = categoryName[interactionKey]

	sortRows = []
	teased = {}

	for iRow in iRows:
		interactionRowCount = interactionRowCount + 1

		if reverse:
			markerKey1 = iRow[participantCol]
			markerKey2 = iRow[organizerCol]
			term = VocabUtils.getSynonym(iRow[termCol],
				'related participant')
		else:
			markerKey1 = iRow[organizerCol]
			markerKey2 = iRow[participantCol]
			term = VocabUtils.getSynonym(iRow[termCol],
				'related organizer')

		# flag markers which are teasers for marker 2

		inTeaser = 0
		if teasers.has_key(markerKey1):
			if markerKey2 in teasers[markerKey1]:
				inTeaser = 1

		row = [
			miGenerator.getKey( (iRow[relationshipCol], reverse) ),
			markerKey1,
			markerKey2,
			MarkerUtils.getSymbol(markerKey2),
			MarkerUtils.getPrimaryID(markerKey2),
			category,
			term,
			VocabUtils.getTerm(iRow[qualifierCol]),
			VocabUtils.getAbbrev(iRow[evidenceCol]),
			iRow[refsCol],
			ReferenceUtils.getJnumID(iRow[refsCol]),
			interactionRowCount,
			inTeaser,
			reverse,
			]

		rows.append(row)

		# note that we use intern() here to share a single instance of
		# the string, keeping memory requirements down

		sortRow = [
			markerKey1,
			intern(term.lower()),
			MarkerUtils.getChromosomeSeqNum(markerKey2),
			MarkerUtils.getStartCoord(markerKey2),
			interactionRowCount
			]
		sortRows.append(sortRow)

#	logger.debug('Collated %d interaction rows, reverse = %d' % (
#		len(rows), reverse))

	# need to modify the sequence numbers now, based on results after we
	# sort the extra 'sortRows' we compiled.

	sortRows.sort()

	i = 0		# tracks sequence num
	seqNum = {}	# maps from interactionRowCount -> sequence num

	for (a, b, c, d, interactionRowCount) in sortRows:
		i = i + 1
		seqNum[interactionRowCount] = i

	for row in rows:
		interactionRowCount = row[-3]
		row[-3] = seqNum[interactionRowCount]

	del sortRows
	del seqNum
	gc.collect()

#	logger.debug ('Updated sequence numbers for %d rows' % len(rows))
	return cols, rows

def expandPropertyRows (pCols, pRows, reverse = 0):
	# produce the property rows for the data file, given the 'pCols'
	# and 'pRows' as retrieved from getPropertyRows().  If 'reverse' is
	# 0, then we produce organizer-to-participant rows.  If it is 1, then
	# do produce participant-to-organizer rows.

	cols = [ 'mi_key', 'name', 'value', 'sequence_num' ]
	rows = []

	keyCol = dbAgnostic.columnNumber (pCols, '_Relationship_key')
	nameCol = dbAgnostic.columnNumber (pCols, '_PropertyName_key')
	valueCol = dbAgnostic.columnNumber (pCols, 'value')
	seqNumCol = dbAgnostic.columnNumber (pCols, 'sequenceNum') 

	for pRow in pRows:
		if pRow[nameCol] == 'note':
			propertyName = pRow[nameCol]
		else:
			propertyName = VocabUtils.getTerm(pRow[nameCol])

		row = [
			miGenerator.getKey( (pRow[keyCol], reverse) ),
			propertyName,
			pRow[valueCol],
			pRow[seqNumCol],
			]
		rows.append(row)

#	logger.debug ('Built %d property rows, reverse = %d' % (
#		len(rows), reverse)) 
	return cols, rows

def processMarkers(startMarker, endMarker):
	# retrieve data and write the rows to the data files for organizer
	# markers with keys between 'startMarker' and 'endMarker'

	logger.debug('Beginning with markers %d-%d' % (startMarker, endMarker))

	iCols, iRows = getInteractionRows(startMarker, endMarker)

	# write forward interaction rows

	cols, rows = expandInteractionRows (iCols, iRows, 0)
	interactionFile.writeToFile (cols, cols, rows)
	logger.debug('Wrote %d rows to interaction file' % len(rows))

	del cols
	del rows
	gc.collect()

	# write reversed interaction rows

	cols, rows = expandInteractionRows (iCols, iRows, 1)
	interactionFile.writeToFile (cols, cols, rows)
	logger.debug('Wrote %d reversed rows to interaction file' % len(rows))

	del iCols
	del iRows
	del cols
	del rows
	gc.collect()

	# write forward property rows

	pCols, pRows = getPropertyRows(startMarker, endMarker)

	cols, rows = expandPropertyRows (pCols, pRows, 0)
	propertyFile.writeToFile ( [ Gatherer.AUTO ] + cols, cols, rows)
	logger.debug('Wrote %d rows to property file' % len(rows))

	del cols
	del rows
	gc.collect()

	# write reversed property rows

	cols, rows = expandPropertyRows (pCols, pRows, 1)
	propertyFile.writeToFile ( [ Gatherer.AUTO ] + cols, cols, rows)
	logger.debug('Wrote %d reversed rows to property file' % len(rows))

	del pCols
	del pRows
	del cols
	del rows
	gc.collect()

	miGenerator.forget()

#	logger.debug('Finished with markers %d-%d' % (startMarker, endMarker))
	return

def main():
	# main program - the basic logic of this gatherer

	global interactionFile, propertyFile

	initialize()

	doneMarkers = 0		# count of markers already processed

	startMarker, endMarker, currentMarkers = getMarkerRange()

	while (startMarker != None):
		processMarkers(startMarker, endMarker)
		doneMarkers = doneMarkers + currentMarkers

		logger.debug('Finished %d of %d markers so far (%0.1f%%)' % (
			doneMarkers, allMarkers,
			100.0 * doneMarkers / allMarkers))

		startMarker, endMarker, currentMarkers = \
			getMarkerRange(endMarker)

	# finalize the files

	interactionFile.close()
	propertyFile.close()

	# write the info out so that femover knows which output file goes with
	# which database table

	print '%s %s' % (interactionFile.getPath(), 'marker_interaction')
	print '%s %s' % (propertyFile.getPath(), 'marker_interaction_property')

if __name__ == '__main__':
	main()
