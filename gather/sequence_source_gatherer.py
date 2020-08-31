#!./python
# 
# gathers data for the 'sequenceSource' table in the front-end database

import config
import Gatherer
import StrainUtils
import dbAgnostic
import logger

###--- Global Constants ---###

STRAIN = 'rawStrain'            # constants for fields from SEQ_Sequence_Raw
TISSUE = 'rawTissue'
AGE = 'rawAge'
SEX = 'rawSex'
CELLLINE = 'rawCellLine'

NOT_SPECIFIED = 'Not Specified'

###--- Functions ---###

def notNone (s):
        if s == None:
                return ''
        return s

def initialize():
        cmd0 = '''select _Sequence_key, _Source_key
                        into temp table tmp_seq_source_assoc
                        from seq_source_assoc
                        order by 1'''

        cmd1 = 'create index ssa_idx1 on tmp_seq_source_assoc (_Sequence_key)'
        cmd2 = 'create index ssa_idx2 on tmp_seq_source_assoc (_Source_key)'
        
        dbAgnostic.execute(cmd0)
        dbAgnostic.execute(cmd1)
        dbAgnostic.execute(cmd2)
        logger.debug('Created temp table tmp_seq_source_assoc')
        return
        
###--- Classes ---###

class SequenceSourceGatherer (Gatherer.ChunkGatherer):
        # Is: a data gatherer for the sequenceSource table
        # Has: queries to execute against the source database
        # Does: queries source database for source data for sequences,
        #       collates results, writes tab-delimited text file

        def collateResults (self):
                # Purpose: we override this method to provide handling for
                #       fields where we need raw values

                # collect the raw values for five columns into a dictionary
                # keyed by sequence key, where each value is a dictionary
                # keyed by the global constants for the fields

                # rawValues[seq key] = { fieldname : raw value }
                self.rawValues = {}

                columns = self.results[0][0]
                keyCol = Gatherer.columnNumber (columns, '_Sequence_key')
                strainCol = Gatherer.columnNumber (columns, STRAIN)
                tissueCol = Gatherer.columnNumber (columns, TISSUE)
                ageCol = Gatherer.columnNumber (columns, AGE)
                sexCol = Gatherer.columnNumber (columns, SEX)
                cellLineCol = Gatherer.columnNumber (columns, CELLLINE)

                for row in self.results[0][1]:
                        self.rawValues[row[keyCol]] = {
                                STRAIN : row[strainCol],
                                TISSUE : row[tissueCol],
                                AGE : row[ageCol],
                                SEX : row[sexCol],
                                CELLLINE : row[cellLineCol]
                                }

                self.finalColumns = self.results[-1][0]
                self.finalResults = self.results[-1][1]
                return

        def postprocessResults (self):
                # Purpose: we override this method to provide cached key
                #       lookups, to attempt to give a performance boost

                self.convertFinalResultsToList()

                keyCol = Gatherer.columnNumber (self.finalColumns,
                        '_Sequence_key')
                tissueCol = Gatherer.columnNumber (self.finalColumns,
                        '_Tissue_key')
                sexCol = Gatherer.columnNumber (self.finalColumns,
                        '_Gender_key')
                strainCol = Gatherer.columnNumber (self.finalColumns,
                        'strain')
                ageCol = Gatherer.columnNumber (self.finalColumns,
                        'age')
                cellLineCol = Gatherer.columnNumber (self.finalColumns,
                        'cellLine')

                for r in self.finalResults:

                        seqKey = r[keyCol]
                        if seqKey in self.rawValues:
                                raw = self.rawValues[seqKey]
                        else:
                                raw = None

                        # lookups from voc_term

                        tissue = Gatherer.resolve(r[tissueCol], 'prb_tissue',
                                '_Tissue_key', 'tissue')
                        sex = Gatherer.resolve(r[sexCol])

                        # for any unspecified or unresolved items, update
                        # them with the raw values

                        if raw:
                                toReplace = [ None, '', 'Not Resolved' ]

                                if tissue in toReplace:
                                        tissue = notNone(raw[TISSUE]) + '*'
                                        if tissue == '*':
                                                tissue = NOT_SPECIFIED

                                if sex in toReplace:
                                        sex = notNone(raw[SEX]) + '*'
                                        if sex == '*':
                                                sex = NOT_SPECIFIED

                                if r[strainCol] in toReplace:
                                        r[strainCol] = \
                                                notNone(raw[STRAIN]) + '*'
                                        if r[strainCol] == '*':
                                                r[strainCol] = NOT_SPECIFIED

                                if r[ageCol] in toReplace:
                                        r[ageCol] = notNone(raw[AGE]) + '*'
                                        if r[ageCol] == '*':
                                                r[ageCol] = NOT_SPECIFIED

                                if r[cellLineCol] in toReplace:
                                        r[cellLineCol] = \
                                                notNone(raw[CELLLINE]) + '*'
                                        if r[cellLineCol] == '*':
                                                r[cellLineCol] = NOT_SPECIFIED

                        self.addColumn('tissue', tissue, r, self.finalColumns)
                        self.addColumn('sex', sex, r, self.finalColumns)
                return

        def getMinKeyQuery (self):
                return 'select min(_Sequence_key) from seq_source_assoc'

        def getMaxKeyQuery (self):
                return 'select max(_Sequence_key) from seq_source_assoc'

###--- globals ---###

cmds = [
        '''select _Sequence_key, rawStrain, rawTissue, rawAge, rawSex,
                rawCellLine
        from seq_sequence_raw r
        where _Sequence_key >= %d and _Sequence_key < %d''',

        '''select ssa._Sequence_key,
                s.strain,
                t.strain_id,
                ps._Tissue_key,
                ps._Gender_key,
                ps.age,
                c.term as cellLine
        from tmp_seq_source_assoc ssa
        inner join prb_source ps on (ssa._Source_key = ps._Source_key)
        inner join voc_term c on (ps._CellLine_key = c._Term_key)
        inner join prb_strain s on (ps._Strain_key = s._Strain_key)
        left outer join %s t on (ps._Strain_key = t._Strain_key)
        where ssa._Sequence_key >= %%d and ssa._Sequence_key < %%d''' % StrainUtils.getStrainIDTempTable()
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, '_Sequence_key', 'strain', 'strain_id', 'tissue', 'age', 'sex', 'cellLine',
        ]

# prefix for the filename of the output file
filenamePrefix = 'sequence_source'

# global instance of a sequenceGatherer
gatherer = SequenceSourceGatherer (filenamePrefix, fieldOrder, cmds)
gatherer.setChunkSize(250000)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        initialize()
        Gatherer.main (gatherer)
