#!./python
# 
# gathers data for the 'marker_qtl_experiments' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class MarkerQtlExperimentsGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the marker_qtl_experiments table
        # Has: queries to execute against the source database
        # Does: queries the source database for notes for QTL mapping
        #       experiments for QTL markers, collates results, writes
        #       tab-delimited text file
        
        def getMarkerNotes(self):
                """
                return list of marker QTL notes
                notes = [(marker key, expt key, jnumID, note type, note),...]
                """
                
                cols, rows = self.results[0]
                
                markerCol = Gatherer.columnNumber (cols, '_Marker_key')
                exptCol = Gatherer.columnNumber (cols, '_Expt_key')
                jnumCol = Gatherer.columnNumber (cols, 'jnumid')
                noteCol = Gatherer.columnNumber (cols, 'note')
                noteTypeCol = Gatherer.columnNumber (cols, 'exptType')
                refNoteCol = Gatherer.columnNumber (cols, 'ref_note')
                
                notes = set([])
                
                # add marker notes
                for row in rows:
                        markerKey = row[markerCol]
                        exptKey = row[exptCol]
                        jnumID = row[jnumCol]
                        note = row[noteCol]
                        noteType = row[noteTypeCol]
                        refNote = row[refNoteCol]

                        notes.add((markerKey, exptKey, jnumID, noteType, note, refNote))
                        
                return notes
        
        

        def collateResults (self):

                # notes = [(marker key, expt key, jnumID, note type, note),...]
                notes = self.getMarkerNotes()

                notes = list(notes)
                notes.sort()

                seqNum = 0

                self.finalColumns = [ '_Marker_key', '_Expt_key', 'accID',
                        'note', 'ref_note', 'noteType', 'sequenceNum' ]
                self.finalResults = []

                for (markerKey, exptKey, jnumID, noteType, note, refNote) in notes:
                        seqNum = seqNum + 1

                        self.finalResults.append ( [ markerKey, exptKey,
                                jnumID,
                                note,
                                refNote,
                                noteType,
                                seqNum] )
                return

###--- globals ---###

cmds = [
        # 0. QTL marker notes and associated ref notes
        '''select mem._marker_key,
                me._expt_key,
                me._refs_key,
                ref_acc.accid jnumid,
                men.note,
                me.expttype,
                mn.note ref_note
        from mld_expt_marker mem
        join mld_expts me on
                me._expt_key=mem._expt_key
        join mld_expt_notes men on
                men._expt_key = me._expt_key
        join acc_accession ref_acc on
                ref_acc._object_key=me._refs_key
                and ref_acc._mgitype_key=1
                and ref_acc.preferred=1
                and ref_acc.prefixpart='J:'
        left outer join mld_notes mn on
                mn._refs_key=me._refs_key
        where me.exptType in ('TEXT-QTL', 'TEXT-QTL-Candidate Genes')
        ''',
        
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, '_Marker_key', '_Expt_key', 'accID', 'note', 'ref_note',
        'noteType', 'sequenceNum',
        ]

# prefix for the filename of the output file
filenamePrefix = 'marker_qtl_experiments'

# global instance of a MarkerQtlExperimentsGatherer
gatherer = MarkerQtlExperimentsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
