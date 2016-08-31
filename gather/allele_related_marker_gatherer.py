#!/usr/local/bin/python
# 
# gathers data for the 'allele_related_marker' and 'allele_arm_property'
# tables in the front-end database.  The tables are patterned very closely
# after the marker interaction tables, so this gatherer is customized from the
# marker_interaction gatherer.  The side benefit is that if this data set
# grows very large, we are already focused on memory usage.

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

mutationInvolvesKey = 1003	# _Category_key for 'mutation involves'
expressedComponentKey = 1004	# _Category_key for 'expressed component'

maxRowCount = 100000	# max number of relationship rows to process in memory

rowsPerAllele = {}	# maps allele key -> count of relationship rows with
			# ...this allele as organizer

maxAlleleKey = None	# maximum (organizer) allele key to process

categoryName = {}	# category key -> category name

notePropertyName = 'note'	# name of property for relationship notes

relationshipRowCount = 0	# count of rows for relationships

allAlleles = 0		# count of all alleles which are organizers

teasers = None		# dictionary of allele keys, referring to a list of
			# marker keys for its teaser markers

mutationInvolvesCount = 0	# count of 'mutation involves' relationships
expressedComponentCount = 0	# count of 'expressed component' relationships

# generator for arm_key values, based on relationship key & reversed flag

armGenerator = KeyGenerator.KeyGenerator('allele_related_marker')

# output files

relationshipFile = OutputFile.OutputFile ('allele_related_marker')
propertyFile = OutputFile.OutputFile ('allele_arm_property')

###--- Functions ---###

def initialize():
	# set up global variables needed for processing

	global maxAlleleKey, rowsPerAllele, allAlleles, teasers

	# find maximum allele key with interactions (with current markers)

	cmd2 = '''select max(r._Object_key_1)
		from mgi_relationship r, mrk_marker m
		where r._Category_key in (%d, %d)
		and r._Object_key_2 = m._Marker_key
		and m._Marker_Status_key = 1''' % (mutationInvolvesKey,
			expressedComponentKey)

	(cols, rows) = dbAgnostic.execute(cmd2)
	maxAlleleKey = rows[0][0]
	logger.debug('Max allele key w/ relationships: %d' % maxAlleleKey)

	# find number of relationships per allele, where the allele is the
	# organizer and the markers are current (not withdrawn)

	cmd3 = '''select r._Object_key_1, count(1) as ct
		from mgi_relationship r, mrk_marker m
		where r._Category_key in (%d, %d)
		and r._Object_key_2 = m._Marker_key
		and m._Marker_Status_key = 1
		group by r._Object_key_1''' % (mutationInvolvesKey,
			expressedComponentKey)

	(cols, rows) = dbAgnostic.execute(cmd3)
	keyCol = dbAgnostic.columnNumber (cols, '_Object_key_1')
	ctCol = dbAgnostic.columnNumber (cols, 'ct')

	for row in rows:
		rowsPerAllele[row[keyCol]] = row[ctCol]

	allAlleles = len(rowsPerAllele)

	logger.debug('Got relationship counts for %d alleles' % allAlleles)

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

def getNextAlleleKey(alleleKey):
	# get the next allele key higher than the given 'alleleKey' which
	# is the organizer for relationships.  Returns None if there are no
	# higher allele keys that have relationships.

	k = alleleKey + 1
	while not rowsPerAllele.has_key(k):
		k = k + 1

		if k > maxAlleleKey:
			return None
	return k

def getAlleleRange(previousMax = 0):
	# get a (start allele key, end allele key, number of alleles) triple,
	# identifying the next allele range to process, where the start allele
	# key will be the next allele with relationships beyond the given
	# previousMax allele key. Returns (None, None, 0) if there are no more
	# alleles to process.

	currentAlleles = 0

	startAllele = getNextAlleleKey(previousMax)
	if startAllele == None:
		return None, None, 0

	endAllele = startAllele		# assume only one marker fits

	currentAlleles = 1

	soFar = rowsPerAllele[startAllele]

	nextAllele = getNextAlleleKey(startAllele)

	if nextAllele == None:
		return startAllele, startAllele, currentAlleles

	nextCount = rowsPerAllele[nextAllele]

	while (soFar + nextCount <= maxRowCount):
		currentAlleles = currentAlleles + 1

		soFar = soFar + nextCount
		endAllele = nextAllele

		nextAllele = getNextAlleleKey(nextAllele)
		if nextAllele != None:
			nextCount = rowsPerAllele[nextAllele]
		else:
			break

	return startAllele, endAllele, currentAlleles

def getTeaserMarkers():
	# get a dictionary where each allele key refers to a list of its
	# teaser markers (up to three).  (Teaser markers are for 'mutation
	# involves' relationships, not 'expressed component'.)
	# Returns: { allele key : [ teaser marker 1, ... teaser marker 3 ] }

	# to keep memory requirements down, we will go through the set of
	# records in chunks by allele key. 

	# allele key : list of up to three teaser marker keys (to be returned)
	teasers = {}

	alleleKeys = rowsPerAllele.keys()
	alleleKeys.sort()

	logger.debug('Need to find M.I. teasers for %d alleles from %d to %d' \
		% (len(alleleKeys), alleleKeys[0], alleleKeys[-1]))

	# let's create our groups of alleles, one group for each execution of
	# the query

	maxAllelesPerGroup = 20
	groups = []
	group = []
	groupRows = 0
	groupAlleles = 0

	for alleleKey in alleleKeys:
		rowCount = rowsPerAllele[alleleKey]

		# if we can fit this allele's rows into our existing group,
		# then do it

		if groupRows + rowCount <= maxRowCount:
			group.append(alleleKey)
			groupRows = groupRows + rowCount

			# if adding this allele made our group hit its maximum
			# number of alleles, save this group and start a new
			# one

			if len(group) == maxAllelesPerGroup:
				groups.append(group)
				group = []
				groupRows = 0

		# special case - if just this allele is more than the number
		# of rows per group, then add a special group just for this
		# one and continue on with our current group.

		elif rowCount > maxRowCount:
			groups.append ( [ alleleKey ] )

		# otherwise, we need to save the current group and start a new
		# one with the new allele

		else:
			groups.append(group)

			group = [ alleleKey ]
			groupRows = rowCount

	# if we (as is likely) end with a non-empty group, then add it to our
	# set of groups, too.

	if group:
		groups.append(group)

	# Now, we have our groups of alleles broken down into bite-size chunks
	# we can use in queries.

	for group in groups:
		alleles = ','.join(map(str, group))

		# allele key : { related marker key : 1 }
		relatedMarkers = {}

		# gather the related markers where the alleles in our group
		# are the organizers (and the markers are current)

		cmd1 = '''select r._Object_key_1, r._Object_key_2
			from mgi_relationship r, mrk_marker m
			where r._Category_key = %d
			and r._Object_key_2 = m._Marker_key
			and m._Marker_Status_key = 1
			and r._Object_key_1 in (%s)''' % (mutationInvolvesKey,
				alleles)

		(cols1, rows1) = dbAgnostic.execute(cmd1)

		organizerCol = dbAgnostic.columnNumber(cols1, '_Object_key_1')
		participantCol = dbAgnostic.columnNumber(cols1, '_Object_key_2')

		for row in rows1:
			organizer = row[organizerCol]
			participant = row[participantCol]

			if not relatedMarkers.has_key(organizer):
				relatedMarkers[organizer] = {}
			relatedMarkers[organizer][participant] = 1

		del cols1
		del rows1
		gc.collect()

		# now for each allele, we need to sort its related markers and
		# identify the three that will be its teasers.

		for allele in group:
			markerList = []

			# if the allele had no mutation-involves relationships
			# (only expressed-component), then skip it

			if not relatedMarkers.has_key(allele):
				continue

			for relatedMarker in relatedMarkers[allele].keys():
			    markerList.append ( (
				MarkerUtils.getChromosomeSeqNum(relatedMarker),
				MarkerUtils.getStartCoord(relatedMarker),
				relatedMarker) )

			markerList.sort()

			teasers[allele] = []
			for (sn, sc, key) in markerList[:3]:
				teasers[allele].append(key)

		del relatedMarkers
		del markerList
		gc.collect()

	del groups
	gc.collect()

	ct = 0
	for key in teasers.keys():
		ct = ct + len(teasers[key])

	logger.debug('Returning %d M.I. teasers for %d alleles' % (ct,
		len(teasers)))
	return teasers

def getRelationshipRows(startAllele, endAllele):
	# get the basic relationship rows for organizer alleles with keys
	# between 'startAllele' and 'endAllele'

	cmd = '''select r._Relationship_key,
			r._Object_key_1 as allele_key,
			r._Object_key_2 as marker_key,
			r._Category_key,
			r._Refs_key,
			r._RelationshipTerm_key,
			r._Qualifier_key,
			r._Evidence_key
		from mgi_relationship r, mrk_marker m
		where r._Category_key in (%d, %d)
			and r._Object_key_1 >= %d
			and r._Object_key_1 <= %d
			and r._Object_key_2 = m._Marker_key
			and m._Marker_Status_key = 1
		order by r._Object_key_1''' % (mutationInvolvesKey,
			expressedComponentKey, startAllele, endAllele)

	(cols, rows) = dbAgnostic.execute(cmd)

	logger.debug ('Got %d relationships for alleles %d-%d' % (
		len(rows), startAllele, endAllele))
	return cols, rows

def getPropertyRows(startAllele, endAllele):
	# get the property rows for organizer alleles with keys between
	# 'startAllele' and 'endAllele'

	# properties for relationships in our allele range, where the
	# associated markers are current or iterim

	cmd1 = '''select p._Relationship_key,
			p._PropertyName_key,
			p.value,
			p.sequenceNum
		from mgi_relationship_property p,
			mgi_relationship r,
			mrk_marker m
		where p._Relationship_key = r._Relationship_key
			and r._Category_key in (%d, %d)
			and r._Object_key_1 >= %d
			and r._Object_key_1 <= %d
			and r._Object_key_2 = m._Marker_key
			and m._Marker_Status_key = 1
		order by p._Relationship_key, p.sequenceNum''' % (
			mutationInvolvesKey, expressedComponentKey,
			startAllele, endAllele)

	(cols1, rows1) = dbAgnostic.execute(cmd1)

	logger.debug('Got %d properties for alleles %d-%d' % (
		len(rows1), startAllele, endAllele))

	# notes for relationships in our allele range, where the associated
	# markers are current

	cmd2 = '''select r._Relationship_key,
			c._Note_key,
			c.sequenceNum,
			c.note
		from mgi_relationship r,
			mgi_notetype t,
			mgi_note n,
			mgi_notechunk c,
			mrk_marker m
		where r._Relationship_key = n._Object_key
			and r._Category_key in (%d, %d)
			and t._NoteType_key = n._NoteType_key
			and t._MGIType_key = 40
			and n._Note_key = c._Note_key
			and r._Object_key_1 >= %d
			and r._Object_key_1 <= %d
			and r._Object_key_2 = m._Marker_key
			and m._Marker_Status_key = 1
		order by r._Relationship_key, c._Note_key, c.sequenceNum''' % (
			mutationInvolvesKey, expressedComponentKey,
			startAllele, endAllele)

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

	logger.debug('Added %d notes as properties for alleles %d-%d' % (
		len(notes), startAllele, endAllele))

	del cols2
	del rows2
	del notes
	gc.collect()

	return cols1, rows1

def expandRelationshipRows(iCols, iRows):
	# produce the interaction rows for the data file, given the 'iCols'
	# and 'iRows' as retrieved from getRelationshipRows(). 

	global relationshipRowCount, expressedComponentCount
	global mutationInvolvesCount

	cols = [ 'arm_key', 'marker_key', 'interacting_marker_key',
		'interacting_marker_symbol', 'interacting_marker_id',
		'relationship_category', 'relationship_term',
		'qualifier', 'evidence_code', 'reference_key',
		'jnum_id', 'sequence_num', 'in_teaser' ]
	rows = []

	relationshipCol = dbAgnostic.columnNumber(iCols, '_Relationship_key')
	organizerCol = dbAgnostic.columnNumber(iCols, 'allele_key')
	participantCol = dbAgnostic.columnNumber(iCols, 'marker_key')
	refsCol = dbAgnostic.columnNumber(iCols, '_Refs_key')
	termCol = dbAgnostic.columnNumber(iCols, '_RelationshipTerm_key')
	qualifierCol = dbAgnostic.columnNumber(iCols, '_Qualifier_key')
	evidenceCol = dbAgnostic.columnNumber(iCols, '_Evidence_key')
	categoryCol = dbAgnostic.columnNumber(iCols, '_Category_key')

	sortRows = []
	teased = {}	# only flag each teaser marker once

	for iRow in iRows:
		relationshipRowCount = relationshipRowCount + 1

		alleleKey = iRow[organizerCol]
		markerKey = iRow[participantCol]
		term = VocabUtils.getSynonym(iRow[termCol],
			'related organizer')

		categoryKey = iRow[categoryCol]
		category = categoryName[categoryKey]

		# flag markers which are teasers for the allele (only for
		# 'mutation involves' relationships)

		inTeaser = 0
		if categoryKey == mutationInvolvesKey:
		    mutationInvolvesCount = 1 + mutationInvolvesCount

		    if teasers.has_key(alleleKey):
			if markerKey in teasers[alleleKey]:

				# This marker should be flagged as a teaser
				# for this allele, but we should only flag one
				# of the rows for this allele/marker pair.

				if not teased.has_key(alleleKey):
					inTeaser = 1
					teased[alleleKey] = [ markerKey ]

				elif markerKey not in teased[alleleKey]:
					inTeaser = 1
					teased[alleleKey].append(markerKey)

		elif categoryKey == expressedComponentKey:
		    expressedComponentCount = 1 + expressedComponentCount

		row = [
			armGenerator.getKey( iRow[relationshipCol] ),
			alleleKey,
			markerKey,
			MarkerUtils.getSymbol(markerKey),
			MarkerUtils.getPrimaryID(markerKey),
			category,
			term,
			VocabUtils.getTerm(iRow[qualifierCol]),
			VocabUtils.getAbbrev(iRow[evidenceCol]),
			iRow[refsCol],
			ReferenceUtils.getJnumID(iRow[refsCol]),
			relationshipRowCount,
			inTeaser,
			]

		rows.append(row)

		sortRow = [
			alleleKey,
			MarkerUtils.getChromosomeSeqNum(markerKey),
			MarkerUtils.getStartCoord(markerKey),
			relationshipRowCount
			]
		sortRows.append(sortRow)

	# need to modify the sequence numbers now, based on results after we
	# sort the extra 'sortRows' we compiled.

	sortRows.sort()

	i = 0		# tracks sequence num
	seqNum = {}	# maps from relationshipRowCount -> sequence num

	for (a, b, c, relationshipRowCount) in sortRows:
		i = i + 1
		seqNum[relationshipRowCount] = i

	for row in rows:
		relationshipRowCount = row[-2]
		row[-2] = seqNum[relationshipRowCount]

	del sortRows
	del seqNum
	gc.collect()

	return cols, rows

def expandPropertyRows (pCols, pRows):
	# produce the property rows for the data file, given the 'pCols'
	# and 'pRows' as retrieved from getPropertyRows().

	cols = [ 'arm_key', 'name', 'value', 'sequence_num' ]
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
			armGenerator.getKey( pRow[keyCol] ),
			propertyName,
			pRow[valueCol],
			pRow[seqNumCol],
			]
		rows.append(row)
	return cols, rows

def processAlleles(startAllele, endAllele):
	# retrieve data and write the rows to the data files for organizer
	# alleles with keys between 'startAllele' and 'endAllele'

	logger.debug('Beginning with alleles %d-%d' % (startAllele, endAllele))

	iCols, iRows = getRelationshipRows(startAllele, endAllele)

	# write forward relationship rows

	cols, rows = expandRelationshipRows (iCols, iRows)
	relationshipFile.writeToFile (cols, cols, rows)
	logger.debug('Wrote %d rows to relationship file' % len(rows))

	del cols
	del rows
	gc.collect()

	# write property rows

	pCols, pRows = getPropertyRows(startAllele, endAllele)

	cols, rows = expandPropertyRows (pCols, pRows)
	propertyFile.writeToFile ( [ Gatherer.AUTO ] + cols, cols, rows)
	logger.debug('Wrote %d rows to property file' % len(rows))

	del cols
	del rows
	gc.collect()

	armGenerator.forget()
	return

def main():
	# main program - the basic logic of this gatherer

	global relationshipFile, propertyFile

	initialize()

	doneAlleles = 0		# count of alleles already processed

	startAllele, endAllele, currentAlleles = getAlleleRange()

	while (startAllele != None):
		processAlleles(startAllele, endAllele)
		doneAlleles = doneAlleles + currentAlleles

		logger.debug('Finished %d of %d alleles so far (%0.1f%%)' % (
			doneAlleles, allAlleles,
			100.0 * doneAlleles / allAlleles))

		startAllele, endAllele, currentAlleles = \
			getAlleleRange(endAllele)

	# finalize the files

	relationshipFile.close()
	propertyFile.close()

	# final debugging output

	logger.debug('Found %d "mutation involves" rows' % \
		mutationInvolvesCount)
	logger.debug('Found %d "expressed component" rows' % \
		expressedComponentCount)

	# write the info out so that femover knows which output file goes with
	# which database table

	print '%s %s' % (relationshipFile.getPath(), 'allele_related_marker')
	print '%s %s' % (propertyFile.getPath(), 'allele_arm_property')

if __name__ == '__main__':
	main()
