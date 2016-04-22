#!/usr/local/bin/python
# 
# gathers data for the 'marker_qtl_experiments' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class MarkerQtlExperimentsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_qtl_experiments table
	# Has: queries to execute against the source database
	# Does: queries the source database for notes for QTL mapping
	#	experiments for QTL markers, collates results, writes
	#	tab-delimited text file
        
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
                
                notes = set([])
                
                # add marker notes
                for row in rows:
                        markerKey = row[markerCol]
                        exptKey = row[exptCol]
                        jnumID = row[jnumCol]
                        note = row[noteCol]
                        noteType = row[noteTypeCol]

                        notes.add((markerKey, exptKey, jnumID, noteType, note))
                        
                return notes
        
        
        def getReferenceNotes(self):
                """
                return list of QTL reference notes
                notes = [(marker key, expt key, jnumID, note type, note),...]
                """
                
                cols, rows = self.results[1]
                
                markerCol = Gatherer.columnNumber (cols, '_Marker_key')
                exptCol = Gatherer.columnNumber (cols, '_Expt_key')
                jnumCol = Gatherer.columnNumber (cols, 'jnumid')
                noteCol = Gatherer.columnNumber (cols, 'note')
                
                notes = set([])
                
                # add marker notes
                for row in rows:
                        markerKey = row[markerCol]
                        exptKey = row[exptCol]
                        jnumID = row[jnumCol]
                        note = row[noteCol]

                        notes.add((markerKey, exptKey, jnumID, "Reference Note", note))
                        
                return notes
        

	def collateResults (self):

		# notes = [(marker key, expt key, jnumID, note type, note),...]
		notes = set([])
                
                notes = notes.union(self.getMarkerNotes())
                
                notes = notes.union(self.getReferenceNotes())


                notes = list(notes)
                notes.sort()

		seqNum = 0

		self.finalColumns = [ '_Marker_key', '_Expt_key', 'accID',
			'note', 'noteType', 'sequenceNum' ]
		self.finalResults = []

		for (markerKey, exptKey, jnumID, noteType, note) in notes:
			seqNum = seqNum + 1

			self.finalResults.append ( [ markerKey, exptKey,
				jnumID,
				note,
				noteType, seqNum] )
		return

###--- globals ---###

cmds = [
        # 0. QTL marker notes
	'''select mem._Marker_key, 
	        me._Expt_key, 
	        me._Refs_key, 
	        ac.accID jnumid,
		men.note, 
		me.exptType
	from MLD_Expt_Marker mem, MLD_Expts me, MLD_Expt_Notes men,
		ACC_Accession ac
	where mem._Expt_key = me._Expt_key 
		and me._Expt_key = men._Expt_key
		and me.exptType in ('TEXT-QTL', 'TEXT-QTL-Candidate Genes')
		and me._Refs_key = ac._Object_key
		and ac._MGIType_key = 1
		and ac.prefixPart = 'J:'
		and ac.preferred = 1
	order by mem._Marker_key, mem.sequenceNum, me._Expt_key,
		men.sequenceNum''',
                
        # 1. Reference notes by marker
        '''select mem._marker_key,
                me._expt_key,
                me._refs_key,
                ref_acc.accid jnumid,
                mn.note,
                me.expttype
        from mld_expt_marker mem
        join mld_expts me on
                me._expt_key=mem._expt_key
        join mld_notes mn on
                mn._refs_key=me._refs_key
        join acc_accession ref_acc on
                ref_acc._object_key=me._refs_key
                and ref_acc._mgitype_key=1
                and ref_acc.preferred=1
                and ref_acc.prefixpart='J:'
        where me.exptType in ('TEXT-QTL')
        ''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', '_Expt_key', 'accID', 'note',
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
