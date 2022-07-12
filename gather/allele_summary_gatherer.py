#!./python
# 
# This gatherer creates all the extra tables needed for the allele summary 
#
# Author: kstone - dec 2013
#

import Gatherer
import dbAgnostic
import logger

###--- Constants ---###
MP_ANNOTTYPE_KEY=1028
DO_ANNOTTYPE_KEY=1029

MP_NORMAL_KEY=2181424

MP_SOURCEANNOT_TERM_KEY=13611348,13576001
NORMAL_PHENOTYPE="no abnormal phenotype observed"
NORMAL_PHENOTYPE_TERM_KEY=83412
NOT_ANALYZED="not analyzed"

###--- Classes ---###
class AlleleSummaryGatherer (Gatherer.MultiFileGatherer):
        # Is: a data gatherer for mp_annotation table 
        # Has: queries to execute against the source database

        ###--- Program Flow ---###
        def buildSummarySystems(self):
                cols=["allele_system_key","allele_key","system"]
                rows=[]
                allSysMap = {}
                
                for r in self.results[0][1]:
                        allKey  = r[0]
                        qualKey = r[1]
                        header  = r[2]
                        isNormal = (qualKey == MP_NORMAL_KEY) or (header == NORMAL_PHENOTYPE)

                        # map abnormal headers to allele keys (We don't get here if isNorm == 1)
                        allSysMap.setdefault(allKey,[]).append((header,isNormal))

                uniqueKey=0
                for allKey,headers in allSysMap.items():
                    allNormal = True
                    for header,isNormal in headers:
                        allNormal = allNormal and isNormal

                    if allNormal:
                        uniqueKey+=1
                        rows.append((uniqueKey,allKey,"no abnormal phenotype observed"))
                    else:
                        for header,isNormal in headers:
                            if not isNormal:
                                uniqueKey+=1
                                rows.append((uniqueKey,allKey,header))
                return cols,rows

        def buildSummaryDiseases(self):
                cols=["allele_disease_key","allele_key","disease","do_id","term_key"]
                rows=[]
                uniqueKey=0
                uniqueRows=set([])
                for r in self.results[1][1]:
                        allKey=r[0]
                        disease=r[1]
                        doId=r[2]
                        termKey=r[3]

                        # test uniqueness
                        uniqueId=(allKey,doId)
                        if uniqueId in uniqueRows:
                                continue
                        uniqueRows.add(uniqueId)

                        uniqueKey+=1
                        rows.append((uniqueKey,allKey,disease,doId,termKey))
                return cols,rows

        def buildSummaryGenotypes(self):
                cols=["allele_genotype_key","allele_key","genotype_key"]
                rows=[]
                uniqueKey=0
                for r in self.results[2][1]:
                        allKey=r[0]
                        genoKey=r[1]

                        uniqueKey+=1
                        rows.append((uniqueKey,allKey,genoKey))
                return cols,rows

        # here is defined the high level script for building all these tables
        # by gatherer convention we create tuples of (cols,rows) for every table we want to create, and append them to self.output
        def buildRows (self):
                logger.debug("building summary system rows")
                systemCols,systemRows=self.buildSummarySystems()

                logger.debug("building summary disease rows")
                diseaseCols,diseaseRows=self.buildSummaryDiseases()

                logger.debug("building summary genotype rows")
                genoCols,genoRows=self.buildSummaryGenotypes()

                logger.debug("done. outputing to file")
                self.output=[(systemCols,systemRows),(diseaseCols,diseaseRows),(genoCols,genoRows)]

        # this is a function that gets called for every gatherer
        def collateResults (self):
                # process all queries
                self.buildRows()

###--- MGD Query Definitions---###
# all of these queries get processed before collateResults() gets called
cmds = [
        # 0. get all MP terms to allele_keys
        '''
        with ancestors as 
            (select
                mp._term_key as _mp_key,
                mp2._term_key as _ancestor_key
            from
                VOC_Term mp,
                DAG_Closure dc,
                VOC_Term mp2
            where
                mp._vocab_key = 5
            and dc._descendentobject_key = mp._term_key
            and dc._ancestorobject_key = mp2._term_key

            union

            select
                mp._term_key as _mp_key,
                mp._term_key as _ancestor_key
            from
                VOC_Term mp
            where
                mp._vocab_key = 5),
        
        mp2headers as (
            select
                a._mp_key,
                a._ancestor_key,
                s.synonym as label
            from ancestors a, mgi_synonym s, voc_term vt
            where a._ancestor_key= s._object_key
            and s._object_key = vt._term_key
            and s._synonymtype_key = 1022
            order by a._mp_key)

        SELECT distinct
            va._object_key as _allele_key,
            va._qualifier_key,
            mh.label
        from
            voc_annot va,
            mp2headers mh,
            voc_term vt
        where va._annottype_key = %d
        and va._term_key = mh._mp_key
        and va._term_key = vt._term_key
        order by _allele_key, label
        ''' % MP_ANNOTTYPE_KEY,
        # 1. get all diseases to allele_keys
        '''
        select distinct
                va._object_key as _allele_key,
                vt.term,
                aa.accID as doId,
                vt._term_key
        from
                VOC_Annot va,
                VOC_Term vt,
                ACC_Accession aa,
                VOC_Term vq
        where 
                va._AnnotType_key = %d
                AND va._Qualifier_key = vq._Term_key
                AND vq.term IS null
                AND va._Term_key = vt._Term_key
                AND vt._Term_key = aa._Object_key
                AND aa._MGIType_key = 13 
                AND aa.preferred = 1
                AND aa.private = 0
        '''% (DO_ANNOTTYPE_KEY),
        # 2. get all allele-genotype keys
        '''
        with all_baseannot as (select
                    va._object_key as _allele_key,
                    cast(vp.value as integer) as _annot_key
                from
                    voc_annot va,
                    voc_evidence ve,
                    voc_evidence_property vp

                where
                    va._annottype_key in (%d,%d)
                and ve._annot_key = va._annot_key 
                and vp._annotevidence_key = ve._annotevidence_key
                and vp._propertyterm_key in (13611348,13576001))

        select distinct
            ba._allele_key,
            va._object_key as _genotype_key
        from 
            all_baseannot ba,
            voc_annot va
        where ba._annot_key = va._annot_key
        order by ba._allele_key
        ''' %(DO_ANNOTTYPE_KEY,MP_ANNOTTYPE_KEY),
        ]

###--- Table Definitions ---###
# definition of output files, each as:
#       (filename prefix, list of fieldnames, table name)
files = [
        ('allele_summary_system',
                ['allele_system_key','allele_key','system'],
                'allele_summary_system'),
        ('allele_summary_disease',
                ['allele_disease_key','allele_key','disease','do_id','term_key'],
                'allele_summary_disease'),
        ('allele_summary_genotype',
                ['allele_genotype_key','allele_key','genotype_key'],
                'allele_summary_genotype'),
        ]

# global instance of a AnnotationGatherer
gatherer = AlleleSummaryGatherer (files, cmds)

###--- main program ---###
# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)

