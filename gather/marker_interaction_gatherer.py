#!./python
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
import gc
import ReferenceUtils
import VocabUtils
import OutputFile
import symbolsort
import Checksum
import config

###--- Globals ---###

interactionKey = 1001   # _Category_key for interactions

maxRowCount = 100000    # max number of interaction rows to process in memory

rowsPerMarker = {}      # maps marker key -> count of interaction rows with
                        # ...this marker as organizer

maxMarkerKey = None     # maximum (organizer) marker key to process

categoryName = {}       # category key -> category name

notePropertyName = 'note'       # name of property for relationship notes

interactionRowCount = 0 # count of rows for interactions

allMarkers = 0          # count of all markers which are organizers

teasers = None          # dictionary of marker keys, referring to a list of
                        # marker keys for its teaser markers

badRelationships = {}   # dictionary of relationship keys where the marker
                        # status of at least one marker is "withdrawn" (so we
                        # must skip these relationships)

# generator for mi_key values, based on relationship key & reversed flag

miGenerator = KeyGenerator.KeyGenerator('marker_interaction')

# checksums -- used to track the version of the source data, so we can
# tell whether we need to regenerate the target data files or if we can
# re-use them

checksums = [
        # count of interaction rows and their most recent modification date
        Checksum.Checksum('marker_interaction.rows', config.CACHE_DIR,
                Checksum.hashResults('select max(modification_date), count(1) from mgi_relationship where _Category_key = %d' % interactionKey)
                ),
        # count of markers and the max marker key
        Checksum.Checksum('marker_interaction.markers', config.CACHE_DIR,
                Checksum.hashResults('select max(_Object_key_1), count(distinct _Object_key_1) from mgi_relationship where _Category_key = %d' % interactionKey)
                ),
        # count of interaction properties and their most recent modification date
        Checksum.Checksum('marker_interaction_property.rows', config.CACHE_DIR,
                Checksum.hashResults('''select count(1), max(p.modification_date)
                        from mgi_relationship r, mgi_relationship_property p
                        where r._Relationship_key = p._Relationship_key
                        and r._Category_key = %d''' % interactionKey)
                ),
        ]

# output files

interactionFile = None
propertyFile = None

###--- Functions ---###

def initialize():
        # set up global variables needed for processing

        global maxMarkerKey, rowsPerMarker, allMarkers, teasers
        global badRelationships, interactionFile, propertyFile

        interactionFile = OutputFile.OutputFile ('marker_interaction', dataDir = config.CACHE_DIR, actualName = True)
        propertyFile = OutputFile.OutputFile ('marker_interaction_property', dataDir = config.CACHE_DIR, actualName = True)

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

        # look up the markers that have been withdrawn

        cmd5 = '''select r._Relationship_key
                from mgi_relationship r, mgi_relationship_category c
                where r._Category_key = c._Category_key
                        and c.name = 'interacts_with'
                        and exists (select 1 from mrk_marker m
                                where r._Object_key_1 = m._Marker_key
                                and m._Marker_Status_key = 2)'''

        cmd6 = cmd5.replace('_Object_key_1', '_Object_key_2')

        badRelationships = {}

        for cmd in [ cmd5, cmd6 ]:
                (cols, rows) = dbAgnostic.execute(cmd)
                for row in rows:
                        badRelationships[row[0]] = 1
        
        logger.debug('Found %d relationships with withdrawn markers' % \
                len(badRelationships)) 

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
        while k not in rowsPerMarker:
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

        endMarker = startMarker         # assume only one marker fits

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

        cmd1 = '''select r._Object_key_2, count(1) as ct
                from mgi_relationship r
                where r._Category_key = %d
                        and not exists (select 1 from mrk_marker m
                                where m._Marker_key = r._Object_key_2
                                and m._Marker_key = 2)
                        and not exists (select 1 from mrk_marker n
                                where n._Marker_key = r._Object_key_1
                                and n._Marker_key = 2)
                group by r._Object_key_2''' % interactionKey

        (cols, rows) = dbAgnostic.execute(cmd1)
        keyCol = dbAgnostic.columnNumber (cols, '_Object_key_2')
        ctCol = dbAgnostic.columnNumber (cols, 'ct')

        # marker key -> number of rows involving it (start with organizer rows
        # then add in the participant rows)
        totalRows = copy.copy(rowsPerMarker)

        for row in rows:
                key = row[keyCol]
                ct = row[ctCol]

                if key in totalRows:
                        totalRows[key] = totalRows[key] + ct
                else:
                        # marker is only a participant, never an organizer
                        totalRows[key] = ct

        markerKeys = list(totalRows.keys())
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

        logit = True
        for group in groups:
                markers = ','.join(map(str, group))

                # marker key : { related marker key : 1 }
                relatedMarkers = {}

                # gather the related markers where the ones in our group are
                # the organizers

                cmd1 = '''select r._Object_key_1, r._Object_key_2,
                                r._Relationship_key
                        from mgi_relationship r
                        where r._Category_key = %d
                                and not exists (select 1 from mrk_marker m
                                        where m._Marker_key = r._Object_key_2
                                        and m._Marker_Status_key = 2)
                                and r._Object_key_1 in (%s)''' % (
                                interactionKey, markers)

                (cols1, rows1) = dbAgnostic.execute(cmd1, logit=logit)

                organizerCol = dbAgnostic.columnNumber(cols1, '_Object_key_1')
                participantCol = dbAgnostic.columnNumber(cols1, '_Object_key_2')
                keyCol = dbAgnostic.columnNumber(cols1, '_Relationship_key')

                for row in rows1:
                        if row[keyCol] in badRelationships:
                                # skip relationships with withdrawn markers
                                continue

                        organizer = row[organizerCol]
                        participant = row[participantCol]


                        # skip self-interacting markers
                        if organizer == participant:
                                continue

                        if organizer not in relatedMarkers:
                                relatedMarkers[organizer] = {}
                        relatedMarkers[organizer][participant] = 1

                del cols1
                del rows1
                gc.collect()

                # gather the related markers where the ones in our group are
                # the participants

                cmd2 = '''select distinct _Object_key_1, _Object_key_2,
                                _Relationship_key
                        from mgi_relationship
                        where _Category_key = %d
                        and _Object_key_2 in (%s)''' % (interactionKey,
                                markers)

                (cols2, rows2) = dbAgnostic.execute(cmd2, logit=logit)
                logit = False

                organizerCol = dbAgnostic.columnNumber(cols2, '_Object_key_1')
                participantCol = dbAgnostic.columnNumber(cols2, '_Object_key_2')
                keyCol = dbAgnostic.columnNumber(cols2, '_Relationship_key')

                for row in rows2:
                        if row[keyCol] in badRelationships:
                                # skip relationships with withdrawn markers
                                continue

                        organizer = row[organizerCol]
                        participant = row[participantCol]


                        # skip self-interacting markers
                        if organizer == participant:
                                continue

                        if participant not in relatedMarkers:
                                relatedMarkers[participant] = {}
                        relatedMarkers[participant][organizer] = 1

                del cols2
                del rows2
                gc.collect()

                # now for each marker, we need to sort its related markers and
                # identify the three that will be its teasers.

                for marker in group:
                        if marker not in relatedMarkers:
                                # if marker was withdrawn, it will have no
                                # related markers
                                continue

                        markerList = []

                        rmKeys = list(relatedMarkers[marker].keys())
                        rmKeys.sort()
                        for relatedMarker in rmKeys:
                                markerList.append ( (
                                        MarkerUtils.getSymbol(relatedMarker),
                                        relatedMarker) )

                        markerList.sort (key=lambda x : symbolsort.splitter(x[0]))

                        teasers[marker] = []
                        for (symbol, key) in markerList[:3]:
                                teasers[marker].append(key)

                del relatedMarkers
                del markerList
                gc.collect()

        del groups
        gc.collect()

        ct = 0
        for key in list(teasers.keys()):
                ct = ct + len(teasers[key])

        logger.debug('Returning %d teasers for %d markers' % (ct,len(teasers)))
        return teasers

def getInteractionRows(startMarker, endMarker, logit):
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

        (cols, rows) = dbAgnostic.execute(cmd, logit=logit)

        logger.debug ('Got %d interactions for markers %d-%d' % (
                len(rows), startMarker, endMarker))
        return cols, rows

def getPropertyRows(startMarker, endMarker, logit):
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

        (cols1, rows1) = dbAgnostic.execute(cmd1, logit=logit)

        logger.debug('Got %d properties for markers %d-%d' % (
                len(rows1), startMarker, endMarker))

        # notes for relationships in our marker range (needed for Excel/tab
        # downloads)

        cmd2 = '''select r._Relationship_key,
                        n._Note_key,
                        n.note
                from mgi_relationship r,
                        mgi_notetype t,
                        mgi_note n
                where r._Relationship_key = n._Object_key
                        and r._Category_key = %d
                        and t._NoteType_key = n._NoteType_key
                        and t._MGIType_key = 40
                        and r._Object_key_1 >= %d
                        and r._Object_key_1 <= %d
                order by r._Relationship_key, n._Note_key''' % (
                        interactionKey, startMarker, endMarker)

        (cols2, rows2) = dbAgnostic.execute(cmd2, logit=logit)

        keyCol = dbAgnostic.columnNumber (cols2, '_Relationship_key')
        noteCol = dbAgnostic.columnNumber (cols2, 'note')
        noteKeyCol = dbAgnostic.columnNumber (cols2, '_Note_key')

        notes = {}      # relationship key -> string note

        prevNoteKey = None

        for row in rows2:
                relationshipKey = row[keyCol]
                noteKey = row[noteKeyCol]

                if relationshipKey not in notes:
                        notes[relationshipKey] = row[noteCol]

                elif noteKey == prevNoteKey:
                        notes[relationshipKey] = notes[relationshipKey] + \
                                row[noteCol]

                else:
                        notes[relationshipKey] = notes[relationshipKey] + \
                                '; ' + row[noteCol]

                prevNoteKey = noteKey

#       logger.debug('Collated %d rows into %d notes for markers %d-%d' % (
#               len(rows2), len(notes), startMarker, endMarker))

        # add the notes to the list of other properties

        keyCol = dbAgnostic.columnNumber (cols1, '_Relationship_key')
        nameCol = dbAgnostic.columnNumber (cols1, '_PropertyName_key')
        valueCol = dbAgnostic.columnNumber (cols1, 'value')
        seqNumCol = dbAgnostic.columnNumber (cols1, 'sequenceNum') 

        fields = [ 0, 1, 2, 3 ]
        i = len(rows1)

        noteKeys = list(notes.keys())
        noteKeys.sort()
        for relationshipKey in noteKeys:
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
                # skip relationships involving withdrawn markers
                if iRow[relationshipCol] in badRelationships:
                        continue

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
                if markerKey1 in teasers:
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
                        sys.intern(term.lower()),
                        MarkerUtils.getChromosomeSeqNum(markerKey2),
                        MarkerUtils.getStartCoord(markerKey2),
                        interactionRowCount
                        ]
                sortRows.append(sortRow)

#       logger.debug('Collated %d interaction rows, reverse = %d' % (
#               len(rows), reverse))

        # need to modify the sequence numbers now, based on results after we
        # sort the extra 'sortRows' we compiled.

        def sortKey(a):
                # return sort key for 'a', handling None appropriately for start coord
                (key, term, chrom, coord, rowCount) = a
                if coord == None:
                        coord = -99999
                return (key, term, chrom, coord, rowCount)

        sortRows.sort(key=sortKey)

        i = 0           # tracks sequence num
        seqNum = {}     # maps from interactionRowCount -> sequence num

        for (a, b, c, d, interactionRowCount) in sortRows:
                i = i + 1
                seqNum[interactionRowCount] = i

        for row in rows:
                interactionRowCount = row[-3]
                row[-3] = seqNum[interactionRowCount]

        del sortRows
        del seqNum
        gc.collect()

#       logger.debug ('Updated sequence numbers for %d rows' % len(rows))
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
                # skip relationships involving withdrawn markers
                if pRow[keyCol] in badRelationships:
                        continue

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

#       logger.debug ('Built %d property rows, reverse = %d' % (
#               len(rows), reverse)) 
        return cols, rows

def processMarkers(startMarker, endMarker, logit):
        # retrieve data and write the rows to the data files for organizer
        # markers with keys between 'startMarker' and 'endMarker'

        logger.debug('Beginning with markers %d-%d' % (startMarker, endMarker))

        iCols, iRows = getInteractionRows(startMarker, endMarker, logit)

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

        pCols, pRows = getPropertyRows(startMarker, endMarker, logit)

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

#       logger.debug('Finished with markers %d-%d' % (startMarker, endMarker))
        return

def main():
        # main program - the basic logic of this gatherer

        global interactionFile, propertyFile

        # if checksums all match, bail out without regenerating data files
        if Checksum.allMatch(checksums):
                print('%s/%s %s' % (config.DATA_DIR, 'marker_interaction.rpt', 'marker_interaction'))
                print('%s/%s %s' % (config.DATA_DIR, 'marker_interaction_property.rpt', 'marker_interaction_property'))
                logger.debug('Using cached data files - done')
                return 

        initialize()

        doneMarkers = 0         # count of markers already processed

        startMarker, endMarker, currentMarkers = getMarkerRange()

        logit = True
        while (startMarker != None):
                processMarkers(startMarker, endMarker, logit)
                logit = False
                doneMarkers = doneMarkers + currentMarkers

                logger.debug('Finished %d of %d markers so far (%0.1f%%)' % (
                        doneMarkers, allMarkers,
                        100.0 * doneMarkers / allMarkers))

                startMarker, endMarker, currentMarkers = \
                        getMarkerRange(endMarker)

        # finalize the files

        interactionFile.close()
        propertyFile.close()
        Checksum.updateAll(checksums)

        # write the info out so that femover knows which output file goes with
        # which database table

        print('%s %s' % (interactionFile.getPath(), 'marker_interaction'))
        print('%s %s' % (propertyFile.getPath(), 'marker_interaction_property'))

if __name__ == '__main__':
        main()
