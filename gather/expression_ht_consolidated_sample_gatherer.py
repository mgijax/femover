#!./python
# 
# gathers data for the 'expression_ht_consolidated_sample' table in the front-end database

import Gatherer
import dbAgnostic
import logger
import gc
import utils
import AgeUtils

###--- Globals ---###

AGE = {}
AGE_MIN = {}
AGE_MAX = {}
SEX = {}
STAGE = {}
ORGANISM = {}
WILDTYPE = {}

###--- Functions ---###

def initialize():
        global AGE, AGE_MAX, AGE_MIN, SEX, STAGE, ORGANISM
        
        ageQuery = '''select distinct m._RNASeqSet_key, s.age, s.ageMin, s.ageMax
                from gxd_htsample_rnaseqsetmember m, gxd_htsample s
                where m._Sample_key = s._Sample_key'''

        organismQuery = '''select _Organism_key, commonName from mgi_organism'''

        stageQuery = '''select _Stage_key, stage from gxd_theilerstage'''

        sexQuery = '''select distinct t._Term_key, t.term
                from voc_term t, gxd_htsample h
                where t._Term_key = h._Sex_key'''

        wildtypeQuery = '''select distinct s._RNASeqSet_key, 1 as is_wild_type
            from gxd_htsample_rnaseqset s, gxd_genotype g
            where s._genotype_key = g._genotype_key
            and not exists (
              select 1 
              from mgi_note n 
              where n._notetype_key = 1016 
              and n._object_key = g._genotype_key)
            '''

        utils.fillDictionary('age per consolidated sample', ageQuery, AGE, '_RNASeqSet_key', 'age')
        utils.fillDictionary('ageMin per consolidated sample', ageQuery, AGE_MIN, '_RNASeqSet_key', 'ageMin')
        utils.fillDictionary('ageMax per consolidated sample', ageQuery, AGE_MAX, '_RNASeqSet_key', 'ageMax')
        utils.fillDictionary('organisms', organismQuery, ORGANISM, '_Organism_key', 'commonName', utils.cleanupOrganism)
        utils.fillDictionary('Theiler Stages', stageQuery, STAGE, '_Stage_key', 'stage')
        utils.fillDictionary('sexes', sexQuery, SEX, '_Term_key', 'term')
        utils.fillDictionary('wildtype', wildtypeQuery, WILDTYPE, '_RNASeqSet_key', 'is_wild_type')

        # tweak age values to use abbreviations
        itemList = list(AGE.items())
        itemList.sort()

        for (key, age) in itemList:
                AGE[key] = AgeUtils.getAbbreviation(age)
        return

###--- Classes ---###

class EHCSGatherer (Gatherer.CachingMultiFileGatherer):
        # Is: a data gatherer for the expression_ht_consolidated_sample table
        # Has: queries to execute against the source database
        # Does: queries the source database for consolidated RNA-Seq samples,
        #       collates results, writes tab-delimited text file

        def collateResults(self):
                cols, rows = self.results[0]
                setKeyCol = dbAgnostic.columnNumber(cols, '_RNASeqSet_key')
                exptKeyCol = dbAgnostic.columnNumber(cols, '_Experiment_key')
                organismKeyCol = dbAgnostic.columnNumber(cols, '_Organism_key')
                sexKeyCol = dbAgnostic.columnNumber(cols, '_Sex_key')
                emapaKeyCol = dbAgnostic.columnNumber(cols, '_Emapa_key')
                genotypeKeyCol = dbAgnostic.columnNumber(cols, '_Genotype_key')
                stageKeyCol = dbAgnostic.columnNumber(cols, '_Stage_key')
                noteCol = dbAgnostic.columnNumber(cols, 'note')
                
                for row in rows:
                        self.addRow('expression_ht_consolidated_sample', [ row[setKeyCol], row[exptKeyCol],
                                row[genotypeKeyCol], ORGANISM[row[organismKeyCol]], SEX[row[sexKeyCol]],
                                AGE[row[setKeyCol]], AGE_MIN[row[setKeyCol]], AGE_MAX[row[setKeyCol]],
                                row[emapaKeyCol], STAGE[row[stageKeyCol]], 
                                WILDTYPE.get(row[setKeyCol], 0),
                                row[noteCol], row[setKeyCol],
                                ])

                logger.debug('Processed %d rows' % len(rows))
                gc.collect()
                return
        
###--- globals ---###

cmds = [ '''select distinct m._RNASeqSet_key, s._Experiment_key, s._Organism_key, s._Sex_key, s._Emapa_key,
                        s._Genotype_key, s._Stage_key, s.note
                from gxd_htsample_rnaseqsetmember m, gxd_htsample_rnaseqset s
                where m._RNASeqSet_key = s._RNASeqSet_key
                        and m._RNASeqSet_key >= %d
                        and m._RNASeqSet_key < %d
                ''' ]

# order of fields (from the query results) to be written to the
# output file
files = [
        ('expression_ht_consolidated_sample',
        [ 'consolidated_sample_key', 'experiment_key', 'genotype_key', 'organism', 'sex', 'age',
                        'age_min', 'age_max', 'emapa_key', 'theiler_stage', 'is_wild_type', 'note', 'sequence_num' ],
        [ 'consolidated_sample_key', 'experiment_key', 'genotype_key', 'organism', 'sex', 'age',
                        'age_min', 'age_max', 'emapa_key', 'theiler_stage', 'is_wild_type', 'note', 'sequence_num' ],
        )
]

# global instance of a EHCSGatherer
gatherer = EHCSGatherer (files, cmds)
gatherer.setupChunking(
        'select min(_RNASeqSet_key) from gxd_htsample_rnaseqset',
        'select max(_RNASeqSet_key) from gxd_htsample_rnaseqset',
        50
        )

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        initialize()
        Gatherer.main (gatherer)
