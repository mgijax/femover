#!./python
# 
# gathers data for the 'alleleNote' table in the front-end database

import Gatherer

###--- Constants ---###

DERIVATION_NOTE_TYPE = 1033
DERIVATION_NOTES = 'Derivation'
DRIVER_NOTES = 'Driver'

###--- Classes ---###

class AlleleNoteGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the alleleNote table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for allele notes,
        #       collates results, writes tab-delimited text file

        def collateResults (self):
                # m[alleleKey] = { noteType : note }
                m = {}

                # process query 0 (including most note types)

                keyCol = Gatherer.columnNumber (self.results[0][0], '_Object_key')
                typeCol = Gatherer.columnNumber (self.results[0][0], '_NoteType_key')
                noteCol = Gatherer.columnNumber (self.results[0][0], 'note') 

                for row in self.results[0][1]:
                        alleleKey = row[keyCol]
                        noteType = Gatherer.resolve (row[typeCol], 'mgi_notetype', '_NoteType_key', 'noteType')
                        note = row[noteCol]

                        if alleleKey not in m:
                                m[alleleKey] = { noteType : note }
                        else:
                                m[alleleKey][noteType] = note

                # add in derivation notes from query 1

                cols, rows = self.results[1]

                alleleKeyCol = Gatherer.columnNumber (cols, '_Allele_key')
                noteKeyCol = Gatherer.columnNumber (cols, '_Note_key')
                noteCol = Gatherer.columnNumber (cols, 'note')
                noteType = DERIVATION_NOTES

                for row in rows:
                        alleleKey = row[alleleKeyCol]
                        note = row[noteCol]
                        noteKey = row[noteKeyCol]

                        if alleleKey not in m:
                                m[alleleKey] = { noteType : note }
                        else:
                                m[alleleKey][noteType] = note

                # add in driver notes from query 2

                cols, rows = self.results[2]
                alleleKeyCol = Gatherer.columnNumber (cols, '_Object_key_1')
                noteCol = Gatherer.columnNumber (cols, 'symbol')
                noteType = DRIVER_NOTES

                for row in rows:
                        alleleKey = row[alleleKeyCol]
                        note = row[noteCol]

                        if alleleKey not in m:
                                m[alleleKey] = { noteType : note }
                        else:
                                m[alleleKey][noteType] = note

                # boil it down to the final set of results

                self.finalColumns = [ 'alleleKey', 'noteType', 'note' ]
                self.finalResults = []

                alleleKeys = list(m.keys())
                alleleKeys.sort()

                for alleleKey in alleleKeys:
                        noteTypes = list(m[alleleKey].keys())
                        noteTypes.sort()

                        # make sure to trim off trailing whitespace

                        for noteType in noteTypes:
                                row = [ alleleKey, noteType,
                                        m[alleleKey][noteType].rstrip() ]
                                self.finalResults.append (row)
                return

###--- globals ---###

cmds = [
        # 0. main query to get traditionally-stored allele notes
        '''select mn._Object_key, mn._Note_key, mn._NoteType_key, mn.note
        from mgi_note mn
        where mn._MGIType_key = 11
                and exists (select 1 from all_allele a
                        where a._Allele_key = mn._Object_key)
        order by mn._Object_key, mn._Note_key''',

        # 1. add in derivation notes
        '''select a._Allele_key, n._Note_key, n.note 
        from all_allele_cellLine a, all_cellline mc, 
                mgi_note n
        where a._MutantCellLine_key = mc._CellLine_key 
                and mc._Derivation_key = n._Object_key 
                and n._NoteType_key = %d
        order by a._Allele_key, n._Note_key''' % DERIVATION_NOTE_TYPE,

        # 2. add in driver note
        '''
        select r._Object_key_1, m.symbol
        from MGI_Relationship r, MRK_Marker m
        where r._Category_key = 1006
        and r._Object_key_2 = m._Marker_key
        ''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'alleleKey', 'noteType', 'note', ]

# prefix for the filename of the output file
filenamePrefix = 'allele_note'

# global instance of a AlleleNoteGatherer
gatherer = AlleleNoteGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
