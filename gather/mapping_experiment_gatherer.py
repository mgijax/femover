#!./python
# 
# gathers data for the 'mapping_experiment' table in the front-end database

import Gatherer
import logger

###--- Constants ---###

ALLELE_FLAG = 'Offspring types indicate alleles inherited from F1 parent.\n'
F1_FLAG = 'F1 direction known.\n'

###--- Classes ---###

class MappingExperimentGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the mapping_experiment table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for mapping experiments,
        #       collates results, writes tab-delimited text file

        def postprocessResults(self):
                # For CROSS experiments, two flags are defined that should add pieces to the standard experiment note.
                
                self.convertFinalResultsToList()
                
                noteCol = Gatherer.columnNumber(self.finalColumns, 'exptNote')
                alleleFlagCol = Gatherer.columnNumber(self.finalColumns, 'alleleFromSegParent')
                f1FlagCol = Gatherer.columnNumber(self.finalColumns, 'F1DirectionKnown')

                alleleFlagCount = 0
                f1FlagCount = 0
                
                for row in self.finalResults:
                        if (row[alleleFlagCol] == 1):
                                if row[noteCol] == None:
                                        row[noteCol] = ALLELE_FLAG
                                else:
                                        row[noteCol] = row[noteCol] + ALLELE_FLAG
                                alleleFlagCount = alleleFlagCount + 1
                                        
                        if (row[f1FlagCol] == 1):
                                if row[noteCol] == None:
                                        row[noteCol] = F1_FLAG
                                else:
                                        row[noteCol] = row[noteCol] + F1_FLAG
                                f1FlagCount = f1FlagCount + 1
                                
                logger.debug('Added %d allele notes and %d F1 notes' % (alleleFlagCount, f1FlagCount))
                return
        
###--- globals ---###

cmds = [
        # 0. basic experiment data; notes fields only have one record, so sequenceNum restriction is just an extra
        #       safety measure to make sure we don't get extra records.
        '''select e._Expt_key, e._Refs_key, e.exptType, e.chromosome, 
                a.accID, rn.note as refsNote, en.note as exptNote, cs.alleleFromSegParent, cs.F1DirectionKnown
        from mld_expts e
        inner join acc_accession a on (e._Expt_key = a._Object_key and a._LogicalDB_key = 1
                and a.preferred = 1 and a._MGIType_key = 4)
        left outer join mld_notes rn on (e._Refs_key = rn._Refs_key)
        left outer join mld_expt_notes en on (e._Expt_key = en._Expt_key)
        left outer join mld_matrix mx on (e._Expt_key = mx._Expt_key)
        left outer join crs_cross cs on (mx._Cross_key = cs._Cross_key)
        where e.exptType != 'CONTIG' ''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        '_Expt_key', 'exptType', 'exptNote', '_Refs_key', 'refsNote', 'chromosome', 'accID',
        ]

# prefix for the filename of the output file
filenamePrefix = 'mapping_experiment'

# global instance of a MappingExperimentGatherer
gatherer = MappingExperimentGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
