#!./python
# Purpose: gathers data for the 'statistic' table in front-end database
# Note: This gatherer populates many of the rows for the table, but others will be added in a post-processing
#       step after the main femover run.  This allows for gathering of statistics that are most easily computed
#       from the front-end database produced, rather than the back-end source database.

import dbAgnostic
import Gatherer
import logger
import types
import StrainUtils

###--- helper function ---###

EXTRA_STATISTICS = {}

def getExtraStat(statisticName):
        global EXTRA_STATISTICS
        
        if not EXTRA_STATISTICS:
                fp = open('../data/extra_statistics.txt', 'r')
                lines = fp.readlines()
                fp.close()
                
                for line in lines:
                        if line.strip().startswith('#') or (line.strip() == 0):
                                continue
                        [ name, value ] = line.split('\t')
                        EXTRA_STATISTICS[name] = int(value)
                        
        if statisticName in EXTRA_STATISTICS:
                return EXTRA_STATISTICS[statisticName]
        return None 
        
### Globals ###

# default value, override from database
SNP_BUILD_NUMBER = 'dbSNP'      
cols, rows = dbAgnostic.execute('select snp_data_version from mgi_dbinfo')
if rows:
        SNP_BUILD_NUMBER = rows[0][0]

STATS = {}                      # statistic name : (tooltip, SQL command) or (tooltip, integer value of count)
GROUPS = {}                     # group name : [ statistic names ]

# constant names for groups of statistics (each mini-home page has its own group, etc.)

HOME_PAGE = 'Home Page'
MARKERS_MINI_HOME = 'Markers Mini Home'
MARKERS_STATS_PAGE = 'Stats Page Markers'
PHENOTYPES_MINI_HOME = 'Phenotypes Mini Home'
PHENOTYPES_STATS_PAGE = 'Stats Page Phenotypes'
GXD_MINI_HOME = 'GXD Mini Home'
GXD_STATS_PAGE = 'Stats Page GXD'
GO_MINI_HOME = 'GO Mini Home'
GO_STATS_PAGE = 'Stats Page GO'
RECOMBINASE_MINI_HOME = 'Recombinase Mini Home'
RECOMBINASE_STATS_PAGE = 'Stats Page Recombinase'
ORTHOLOGY_MINI_HOME = 'Orthology Mini Home'
ORTHOLOGY_STATS_PAGE = 'Stats Page Orthology'
POLYMORPHISMS_MINI_HOME = 'Polymorphisms Mini Home'
POLYMORPHISMS_STATS_PAGE = 'Stats Page Polymorphisms'
SEQUENCES_STATS_PAGE = 'Stats Page Sequences'
REFERENCES = 'References'

###--- sequence statistics ---###

SEQ_DNA = 'Mouse nucleotide sequences'
SEQ_RNA = 'Mouse transcript sequences'
SEQ_POLYPEPTIDE = 'Mouse polypeptide sequences'

STATS[SEQ_DNA] = ('', 'SELECT COUNT(1) FROM SEQ_Sequence WHERE _SequenceType_key = 316347 AND _Organism_key = 1')
STATS[SEQ_RNA] = ('', 'SELECT COUNT(1) FROM SEQ_Sequence WHERE _SequenceType_key = 316346 AND _Organism_key = 1')
STATS[SEQ_POLYPEPTIDE] = ('', 'SELECT COUNT(1) FROM SEQ_Sequence WHERE _SequenceType_key = 316348 AND _Organism_key = 1')

GROUPS[SEQUENCES_STATS_PAGE] = [ SEQ_DNA, SEQ_RNA, SEQ_POLYPEPTIDE ]

###--- recombinase statistics ---###

CRE_DRIVERS_IN_ALLELES = 'Drivers in recombinase knock-in alleles'
CRE_DRIVERS_IN_TRANSGENES = 'Drivers in recombinase transgenes'
CRE_ALLELES = 'Recombinase-containing knock-in alleles'
CRE_TRANSGENES = 'Recombinase-containing transgenes'
CRE_TISSUES = 'Tissues in recombinase specificity assays'
CRE_FEATURES = 'Total recombinase transgenes and alleles'

STATS[CRE_DRIVERS_IN_ALLELES] = ('',
        'select count(distinct driverNote) from ALL_Cre_Cache where _Allele_Type_key = 847116')
STATS[CRE_DRIVERS_IN_TRANSGENES] = ('',
        'select count (distinct driverNote) from ALL_Cre_Cache where _Allele_Type_key = 847126')
STATS[CRE_ALLELES] = ('',
        'select count(distinct _Allele_key) from ALL_Cre_Cache where _Allele_Type_key = 847116')
STATS[CRE_TRANSGENES] = ('',
        'select count(distinct _Allele_key) from ALL_Cre_Cache where _Allele_Type_key = 847126')
STATS[CRE_TISSUES] = ('',
        'select count(*) from (select distinct _EMAPA_Term_key, _Stage_key from All_Cre_Cache) as foo')
STATS[CRE_FEATURES] = ('',
        'select count(distinct _Allele_key) from ALL_Cre_Cache')

GROUPS[RECOMBINASE_MINI_HOME] = [ CRE_ALLELES, CRE_TRANSGENES, CRE_FEATURES, CRE_DRIVERS_IN_TRANSGENES,
                CRE_DRIVERS_IN_ALLELES, CRE_TISSUES ]
GROUPS[RECOMBINASE_STATS_PAGE] = GROUPS[RECOMBINASE_MINI_HOME][:]

###--- Gene Ontology (GO) statistics ---###

GO_GENES = 'Mouse genes (protein-coding and non-protein coding) with GO annotations'
GO_GENES_EXPERIMENTAL = 'Mouse genes (protein-coding and non-protein coding) with experimentally-derived in mouse GO annotations'
GO_ANNOTATIONS = 'GO annotations total'
GO_REFERENCES = 'Unique references used for GO annotations'
GO_REF_GENOME = 'Mouse genes selected for GO Reference Genome Project'

STATS[GO_GENES] = ('Annotations based on evidence from mouse experiments, orthology from other organisms, and electronic or other computational methods.',

        '''SELECT COUNT(DISTINCT v._Object_key)
                FROM VOC_Annot v, MRK_Marker m
                WHERE v._AnnotType_key = 1000 AND v._Object_key = m._Marker_key AND m._Marker_Type_key = 1''')

STATS[GO_GENES_EXPERIMENTAL] = ('Annotations based on experiments performed in the mouse or with mouse gene products.',
        '''SELECT COUNT(DISTINCT v._Object_key)
                FROM VOC_Annot v, MRK_Marker m, VOC_Evidence e, VOC_Term t
                WHERE v._AnnotType_key = 1000 AND v._Object_key = m._Marker_key AND m._Marker_Type_key = 1
                        AND v._Annot_key = e._Annot_key AND e._EvidenceTerm_key = t._Term_key
                        AND t.abbreviation IN ('EXP', 'IDA', 'IMP', 'IGI', 'IPI', 'IEP')''')

# copied from qcreports_db/GO_stats.py
STATS[GO_ANNOTATIONS] = ('', 
        '''
        select count(a._Annot_key)
        from VOC_Annot a, VOC_Evidence e, MRK_Marker m
        where a._AnnotType_key  = 1000
        and a._Annot_key = e._Annot_key
        and a._Object_key = m._Marker_key
        and m._Marker_Type_key in (1)
        ''')

STATS[GO_REFERENCES] = ('',
        '''SELECT COUNT(DISTINCT e._Refs_key)
                FROM VOC_Annot v, VOC_Evidence e, ACC_Accession a
                WHERE v._AnnotType_key = 1000 AND v._Annot_key = e._Annot_key
                        AND e._Refs_key = a._Object_key AND a._MGIType_key = 1 AND a._LogicalDB_key = 29''')

#STATS[GO_REF_GENOME] = ('',
#       '''select count(m._Marker_key)
#               from GO_Tracking t, MRK_Marker m, ACC_Accession a
#               where t.isReferenceGene = 1 and t._Marker_key = m._Marker_key
#                       and m._Marker_key = a._Object_key and a._MGIType_key = 2
#                       and a._LogicalDB_key = 1 and a.prefixPart = 'MGI:' and a.preferred = 1''')

GROUPS[GO_MINI_HOME] = [ GO_GENES, GO_GENES_EXPERIMENTAL, GO_ANNOTATIONS, GO_REFERENCES]
GROUPS[GO_STATS_PAGE] = GROUPS[GO_MINI_HOME][:]

###--- Gene Expression Database (GXD) statistics ---###

GXD_GENES_INDEXED = 'Genes studied in expression references'
GXD_GENES_CODED = 'Genes/markers with annotated expression results (all assay types)'
GXD_GENES_CLASSICAL = 'Genes/markers with in situ and blot assay results'
GXD_GENES_RNASEQ = 'Genes/markers with RNA-Seq assay results'
GXD_RESULTS = 'Expression results (all assay types)'
GXD_INSITU_RESULTS = 'In situ assay results'
GXD_BLOT_RESULTS = 'Blot assay results'
GXD_RNASEQ_RESULTS = 'RNA-Seq assay results'
GXD_IMAGES = 'Expression images'
GXD_ASSAYS = 'Expression assays'
GXD_MUTANTS = 'Mouse mutants with expression data'

STATS[GXD_GENES_INDEXED] = ('Genes searchable using the Gene Expression Literature Query', 'SELECT COUNT(DISTINCT _Marker_key) FROM GXD_Index')
STATS[GXD_GENES_CODED] = ('Genes searchable using the Gene Expression Data Query',
        '''SELECT COUNT(m._Marker_key)
                FROM MRK_Marker m
                WHERE m._Organism_key = 1
                AND m._Marker_Status_key in (1,3)
                AND (EXISTS (SELECT 1 FROM GXD_Expression e
                        WHERE m._Marker_key = e._Marker_key
                        AND e.isForGXD = 1)
                        OR
                        EXISTS (SELECT 1 FROM GXD_HTSample_RnaSeqCombined r
                        WHERE m._Marker_key = r._Marker_key)
                )''')
STATS[GXD_GENES_CLASSICAL] = ('',
        '''SELECT COUNT(DISTINCT ge._Marker_key)
                FROM GXD_Expression ge, MRK_Marker mm
                WHERE ge._Marker_key = mm._Marker_key AND mm._Organism_key = 1
                        AND mm._Marker_Status_key IN (1,3) and ge.isForGXD = 1''')
STATS[GXD_GENES_RNASEQ] = ('',
        '''SELECT COUNT(m._Marker_key)
        FROM MRK_Marker m
        WHERE m._Organism_key = 1
        AND m._Marker_Status_key IN (1,3)
        AND EXISTS (SELECT 1 FROM GXD_HTSample_RnaSeqCombined r
          WHERE m._Marker_key = r._Marker_key)''')
STATS[GXD_RESULTS] = ('',
        '''WITH rowcounts AS (SELECT count(1) AS rowcount
        FROM GXD_Expression 
        WHERE isForGXD = 1
        UNION
        SELECT COUNT(1) AS rowcount
        FROM gxd_htsample_rnaseqcombined
        )
        SELECT SUM(rowcount)::INTEGER AS total
        FROM rowcounts''')
STATS[GXD_INSITU_RESULTS] = ('', '''SELECT COUNT(DISTINCT _Expression_key)
        FROM GXD_Expression
        WHERE isForGXD = 1
        AND _AssayType_key in (6, 9, 1)''')
STATS[GXD_BLOT_RESULTS] = ('', '''SELECT COUNT(DISTINCT _Expression_key)
        FROM GXD_Expression
        WHERE isForGXD = 1
        AND _AssayType_key in (2, 3, 4, 5, 8)''')
STATS[GXD_RNASEQ_RESULTS] = ('', 'SELECT COUNT(1) FROM gxd_htsample_rnaseqcombined')
STATS[GXD_IMAGES] = ('',
        '''select count(docount._ImagePane_key)
                from ( select distinct ip._ImagePane_key from IMG_ImagePane ip, IMG_Image i, GXD_Assay a
                        where ip._Image_key = i._Image_key and i.xDim is not null
                                and ip._ImagePane_key = a._ImagePane_key and a._AssayType_key in (1,2,3,4,5,6,8,9)
                        union all
                        select distinct ip._ImagePane_key
                        from IMG_ImagePane ip, IMG_Image i, GXD_InSituResultImage isri, GXD_InSituResult isr,
                                GXD_Specimen s, GXD_Assay a
                        where ip._Image_key = i._Image_key and i.xDim is not null
                                and ip._ImagePane_key = isri._ImagePane_key and isri._Result_key = isr._Result_key
                                and isr._Specimen_key = s._Specimen_key and s._Assay_key = a._Assay_key
                                and a._AssayType_key in (1,2,3,4,5,6,8,9) )
                        AS docount''')
STATS[GXD_ASSAYS] = ('', '''WITH data AS (
        SELECT COUNT(DISTINCT _Assay_key) AS ct
        FROM GXD_Expression WHERE isForGXD = 1
        UNION
        SELECT COUNT(distinct _Experiment_key) AS ct
        FROM GXD_HTSample_RNASeqSet
        )
        SELECT SUM(ct)::INT
        FROM data''')
STATS[GXD_MUTANTS] = ('', '''WITH alleles AS (SELECT DISTINCT a._Allele_key
        FROM GXD_Expression e, GXD_AlleleGenotype g, ALL_Allele a
        WHERE e._Genotype_key = g._Genotype_key AND g._Allele_key = a._Allele_key
        AND a.isWildType = 0 and e.isForGXD = 1
        UNION
        SELECT DISTINCT a._Allele_key
        FROM GXD_HTSample_RNASeqSet e, GXD_AlleleGenotype g, ALL_Allele a
        WHERE e._Genotype_key = g._Genotype_key AND g._Allele_key = a._Allele_key
        AND a.isWildType = 0
        )
        SELECT COUNT(DISTINCT _Allele_key)
        FROM alleles''')

GROUPS[GXD_MINI_HOME] = [ GXD_GENES_INDEXED, GXD_GENES_CODED, GXD_GENES_CLASSICAL, GXD_GENES_RNASEQ, GXD_RESULTS, GXD_INSITU_RESULTS, GXD_BLOT_RESULTS, GXD_RNASEQ_RESULTS, GXD_IMAGES, GXD_ASSAYS, GXD_MUTANTS ]
GROUPS[GXD_STATS_PAGE] = GROUPS[GXD_MINI_HOME][:]

###--- marker statistics ---###

MRK_GENES = 'Genes (including uncloned mutants)'
MRK_GENES_DNA = 'Genes with nucleotide sequence data'
MRK_GENES_PROTEIN = 'Genes with protein sequence data'
MRK_GENES_GO = 'Genes with experimentally-based functional annotations'
MRK_GENES_TRAPPED = 'Genes with gene traps'
MRK_GENES_MAPPED = 'Mapped genes/markers'

STATS[MRK_GENES] = ('',
        '''SELECT COUNT(1)
                FROM MRK_Marker
                WHERE _Organism_key = 1 AND _Marker_Type_key = 1 AND _Marker_Status_key IN (1,3)''')
STATS[MRK_GENES_DNA] = ('',
        '''SELECT COUNT(DISTINCT m._Marker_key)
                FROM MRK_Marker m, SEQ_Marker_Cache s
                WHERE m._Marker_key = s._Marker_key AND m._Organism_key = 1 AND m._Marker_Type_key = 1
                AND m._Marker_Status_key IN (1,3) AND s._SequenceType_key = 316347''')
STATS[MRK_GENES_PROTEIN] = ('',
        '''SELECT COUNT(DISTINCT m._Marker_key)
                FROM MRK_Marker m, sEQ_Marker_Cache s
                WHERE m._Marker_key = s._Marker_key AND m._Organism_key = 1
                        AND m._Marker_Type_key = 1 AND m._Marker_Status_key IN (1,3) AND s._SequenceType_key = 316348''')
STATS[MRK_GENES_GO] = ('',
        '''SELECT COUNT(DISTINCT v._Object_key)
                FROM VOC_Annot v, MRK_Marker m
                WHERE v._AnnotType_key = 1000 AND v._Object_key = m._Marker_key AND m._Marker_Type_key = 1
                AND exists (select 1
                        from voc_annot v2, voc_evidence ve
                        where m._Marker_key = v2._Object_key and v2._AnnotType_key = 1000
                                and v2._Annot_key = ve._Annot_key and ve._EvidenceTerm_key not in (115, 118))''')
STATS[MRK_GENES_TRAPPED] = ('',
        '''select count(distinct a._Marker_key)
                from all_allele a, mrk_marker m
                where a._Allele_Type_key = 847121 and a._Marker_key is not null
                        and a._Marker_key = m._Marker_key and m._Marker_Status_key != 2 and m._Marker_Type_key = 1''')
STATS[MRK_GENES_MAPPED] = ('', 'SELECT COUNT(1) FROM MRK_Location_Cache WHERE startCoordinate is not null')

GROUPS[MARKERS_MINI_HOME] = [ MRK_GENES, MRK_GENES_DNA, MRK_GENES_PROTEIN, MRK_GENES_GO, MRK_GENES_TRAPPED ]
GROUPS[MARKERS_STATS_PAGE] = [ MRK_GENES, MRK_GENES_DNA, MRK_GENES_PROTEIN, MRK_GENES_GO, MRK_GENES_TRAPPED, GXD_GENES_CODED,
        GXD_GENES_CLASSICAL, GXD_GENES_RNASEQ, MRK_GENES_MAPPED ]

###--- orthology statistics ---###

ORTH_GENES = 'Mouse protein coding genes'
ORTH_GENES_WITH_ORTHOLOGS = 'Mouse protein coding genes in homology classes'
ORTH_GENES_WITH_HUMAN = 'Mouse protein coding genes in homology classes with Human genes'
ORTH_GENES_1_TO_1 = 'Mouse protein coding genes in homology classes with one-to-one correspondence of Mouse and Human genes'
ORTH_GENES_WITH_RAT = 'Mouse protein coding genes in homology classes with Rat genes'
ORTH_GENES_WITH_ZFIN = 'Mouse protein coding genes in homology classes with Zebrafish genes'

HOMOLOGY = 9272150
ALLIANCE_DIRECT = 75885739

STATS[ORTH_GENES] = ('',
        '''select count(distinct mcv._Marker_key)
                from MRK_MCV_Cache mcv, MRK_Marker m
                where mcv.term = 'protein coding gene' and mcv.qualifier = 'D' and mcv._Marker_key = m._Marker_key
                and m._Marker_Status_key in (1,3) and m._Organism_key = 1''')
STATS[ORTH_GENES_WITH_ORTHOLOGS] = ('',
        '''select count(distinct mcv._Marker_key)
                from MRK_MCV_Cache mcv, MRK_Marker m, MRK_ClusterMember mcm, MRK_Cluster mc
                where mcv.term = 'protein coding gene' and mcv.qualifier = 'D' and mcv._Marker_key = m._Marker_key
                        and m._Marker_Status_key in (1,3) and m._Organism_key = 1 and m._Marker_key = mcm._Marker_key
                        and mcm._Cluster_key = mc._Cluster_key and mc._ClusterSource_key = %d
                        and mc._ClusterType_key = %d''' % (ALLIANCE_DIRECT, HOMOLOGY))
STATS[ORTH_GENES_1_TO_1] = ('',
        '''select count(distinct mcv._Marker_key)
                from MRK_MCV_Cache mcv, MRK_Marker m, MRK_ClusterMember mcm, MRK_Cluster mc,
                        MRK_ClusterMember ocm, MRK_Marker om
                where mcv.term = 'protein coding gene' and mcv.qualifier = 'D' and mcv._Marker_key = m._Marker_key
                        and m._Marker_Status_key in (1,3) and m._Organism_key = 1 and m._Marker_key = mcm._Marker_key
                        and mcm._Cluster_key = mc._Cluster_key and mc._ClusterSource_key = %d
                        and mc._ClusterType_key = %d and mc._Cluster_key = ocm._Cluster_key
                        and ocm._Marker_key = om._Marker_key and om._Organism_key = 2
                        and not exists (select 1 from MRK_ClusterMember ocm2, MRK_Marker om2, MRK_Cluster oc2
                                where mc._Cluster_key = ocm2._Cluster_key and ocm2._Cluster_key = oc2._Cluster_key
                                        and oc2._ClusterSource_key = %d and oc2._ClusterType_key = %d
                                        and ocm2._Marker_key = om2._Marker_key and om2._Organism_key = 2
                                        and om2._Marker_key != om._Marker_key)
                        and not exists (select 1 from MRK_ClusterMember mcm2, MRK_Marker m2, MRK_Cluster mc2
                                where mc._Cluster_key = mcm2._Cluster_key and mcm2._Cluster_key = mc2._Cluster_key
                                        and mc2._ClusterSource_key = %d and mc2._ClusterType_key = %d
                                        and mcm2._Marker_key = m2._Marker_key and m2._Organism_key = 1
                                        and m2._Marker_key != m._Marker_key)''' % (ALLIANCE_DIRECT, HOMOLOGY,
                                                                                   ALLIANCE_DIRECT, HOMOLOGY,
                                                                                   ALLIANCE_DIRECT, HOMOLOGY))

ORGANISM_QUERY = '''select count(distinct mcv._Marker_key)
                from MRK_MCV_Cache mcv, MRK_Marker m, MRK_ClusterMember mcm, MRK_Cluster mc
                where mcv.term = 'protein coding gene' and mcv.qualifier = 'D' and mcv._Marker_key = m._Marker_key
                        and m._Marker_Status_key in (1,3) and m._Organism_key = 1 and m._Marker_key = mcm._Marker_key
                        and mcm._Cluster_key = mc._Cluster_key and mc._ClusterSource_key = %d
                        and mc._ClusterType_key = %d
                        and exists (select 1 from MRK_ClusterMember ocm, MRK_Marker om, MRK_Cluster oc
                                where mc._Cluster_key = ocm._Cluster_key and ocm._Cluster_key = oc._Cluster_key
                                        and oc._ClusterSource_key = %d and oc._ClusterType_key = %d
                                        and ocm._Marker_key = om._Marker_key and om._Organism_key = %s)''' % (
                                            ALLIANCE_DIRECT, HOMOLOGY, ALLIANCE_DIRECT, HOMOLOGY, '%d')

STATS[ORTH_GENES_WITH_HUMAN] = ('', ORGANISM_QUERY % 2)
STATS[ORTH_GENES_WITH_RAT] = ('', ORGANISM_QUERY % 40)
STATS[ORTH_GENES_WITH_ZFIN] = ('', ORGANISM_QUERY % 84)

GROUPS[ORTHOLOGY_MINI_HOME] = [ ORTH_GENES, ORTH_GENES_WITH_ORTHOLOGS, ORTH_GENES_WITH_HUMAN, ORTH_GENES_1_TO_1,
        ORTH_GENES_WITH_RAT, ORTH_GENES_WITH_ZFIN ]
GROUPS[ORTHOLOGY_STATS_PAGE] = GROUPS[ORTHOLOGY_MINI_HOME][:]

###--- phenotype statistics ---###

MP_ALLELES = 'Mutant alleles in mice'
MP_GENES = 'Genes with mutant alleles in mice'
MP_GENOTYPES = 'Genotypes with phenotype annotations'
MP_DISEASES = 'Human diseases with one or more mouse models'
MP_ALLELES_LINES = 'Mutant alleles in cell lines only'
MP_GENES_LINES = 'Genes with mutant alleles in cell lines only'
MP_TARGETED = 'Total targeted alleles'
MP_GENES_TARGETED = 'Genes with targeted alleles'
MP_RECOMBINASE = 'Total recombinase transgenes and alleles'
MP_GENOTYPES_MODEL = 'Mouse genotypes modeling human diseases'
MP_TERMS = 'Mammalian Phenotype (MP) ontology terms'
MP_ANNOTATIONS = 'MP annotations total'
MP_QTL = 'QTL'

STATS[MP_ALLELES] = ('',
        '''SELECT COUNT(1)
                FROM ALL_Allele
                WHERE isWildType = 0 AND _Allele_Type_key != 847130
                        AND _Allele_Status_key in (847114, 3983021) AND _Transmission_key != 3982953''')
STATS[MP_GENES] = ('',
        '''SELECT COUNT(DISTINCT a._Marker_key)
                FROM ALL_Allele a, MRK_Marker m
                WHERE a.isWildType = 0 AND a._Allele_Type_key != 847130
                        AND a._Allele_Status_key in (847114, 3983021) AND a._Transmission_key != 3982953
                        AND a._Marker_key = m._Marker_key AND m._Marker_Type_key = 1
                        AND not exists (select 1 from VOC_Annot va
                                where va._AnnotType_key = 1011 AND va._Object_key = m._Marker_key and va._Term_key = 6238170)''')
STATS[MP_GENOTYPES] = ('',
        '''SELECT COUNT(DISTINCT g._Genotype_key)
                FROM VOC_Annot v, GXD_AlleleGenotype g, ALL_Allele a
                WHERE v._AnnotType_key = 1002 AND v._Object_key = g._Genotype_key
                        AND g._Allele_key = a._Allele_key AND a._Allele_Type_key != 847130''')
STATS[MP_DISEASES] = ('', 0)                    # compute value during post-processing step
STATS[MP_ALLELES_LINES] = ('',
        '''SELECT COUNT(1)
                FROM ALL_Allele
                WHERE isWildType = 0 AND _Allele_Status_key in (847114, 3983021)
                        AND _Transmission_key = 3982953''')
STATS[MP_GENES_LINES] = ('',
        '''SELECT COUNT(DISTINCT a._Marker_key)
                FROM ALL_Allele a, MRK_Marker m
                WHERE a.isWildType = 0 AND a._Allele_Type_key != 847130
                        AND a._Allele_Status_key in (847114, 3983021) AND a._Marker_key = m._Marker_key
                        AND m._Marker_Type_key not in (6, 9, 10, 3)
                        and not exists (select * from all_allele a2
                                where a2._Marker_key = a._Marker_key and a2._Transmission_key != 3982953
                                        and a2.isWildType = 0 AND a2._Allele_Type_key != 847130
                                        and a2._Allele_Status_key in (847114, 3983021))
                        and exists (select * from all_allele a3
                                where a3._Marker_key = a._Marker_key and a3._Transmission_key = 3982953
                                        and a3.isWildType = 0 AND a3._Allele_Type_key != 847130
                                        and a3._Allele_Status_key in (847114, 3983021))''')
STATS[MP_TARGETED] = ('',
        '''select count(_Allele_key)
                FROM ALL_Allele
                WHERE _Allele_Type_key = 847116 AND _Allele_Status_key in (847114, 3983021)''')
STATS[MP_GENES_TARGETED] = ('',
        '''select count(distinct(_Marker_key))
                from all_allele
                WHERE _Allele_Type_key = 847116 AND _Allele_Status_key in (847114, 3983021)''')
STATS[MP_RECOMBINASE] = ('', 'select count(distinct _Allele_key) from ALL_Cre_Cache')
STATS[MP_GENOTYPES_MODEL] = ('', 0)             # compute value during post-processing step
STATS[MP_TERMS] = ('', 'SELECT COUNT(1) FROM VOC_Term WHERE _Vocab_key = 5 AND isObsolete = 0')
STATS[MP_ANNOTATIONS] = ('', 'SELECT COUNT(1) FROM VOC_Annot v WHERE v._AnnotType_key = 1002')
STATS[MP_QTL] = ('',
        '''SELECT COUNT(1)
                FROM MRK_Marker
                WHERE _Marker_Type_key = 6 AND _Marker_Status_key IN (1,3) AND _Organism_key = 1''')

GROUPS[PHENOTYPES_MINI_HOME] = [ MP_ALLELES, MP_GENES, MP_GENOTYPES, MP_DISEASES ]
GROUPS[PHENOTYPES_STATS_PAGE] = [ MP_ALLELES, MP_ALLELES_LINES, MP_GENES, MP_GENES_LINES, MP_TARGETED,
        MP_GENES_TARGETED, MP_RECOMBINASE, MP_GENOTYPES, MP_DISEASES, MP_GENOTYPES_MODEL, MP_TERMS,
        MP_ANNOTATIONS, MP_QTL ]

###--- reference statistics ---###

REFERENCES = 'References'

STATS[REFERENCES] = ('', 'SELECT COUNT(1) FROM BIB_Refs')
GROUPS[REFERENCES] = [ REFERENCES ]

###--- polymorphism statistics ---###

PLY_REFSNPS = 'RefSNPs'
PLY_STRAINS_SNPS = 'Strains with SNPs'
PLY_RFLP = 'RFLP records'
PLY_PCR = 'PCR polymorphism records'
PLY_GENES = 'Genes with polymorphisms (RFLP, PCR)'
PLY_STRAINS = 'Strains'

STATS[PLY_REFSNPS] = ('From %s' % SNP_BUILD_NUMBER, getExtraStat(PLY_REFSNPS))
STATS[PLY_STRAINS_SNPS] = ('Strains that have been analyzed in %s' % SNP_BUILD_NUMBER, getExtraStat(PLY_STRAINS_SNPS))
STATS[PLY_RFLP] = ('Restriction Fragment Length Polymorphism records',
        '''select count(pr._Reference_key)
                from PRB_Probe pp, PRB_RFLV pr, PRB_Reference ref, VOC_Term vt
                where pp._SegmentType_key = vt._Term_key and pr._Reference_key = ref._Reference_key
                        and ref._Probe_key = pp._Probe_key and vt.term != 'primer' ''')
STATS[PLY_PCR] = ('Polymerase Chain Reaction polymorphism records',
        '''select count(pr._Reference_key)
                from PRB_Probe pp, PRB_RFLV pr, PRB_Reference ref, VOC_Term vt
                where pp._SegmentType_key = vt._Term_key and pr._Reference_key = ref._Reference_key
                        and ref._Probe_key = pp._Probe_key and vt.term = 'primer' ''')
STATS[PLY_GENES] = ('Genes with RFLP and/or PCR polymorphism data',
        '''SELECT COUNT(DISTINCT pr._Marker_key)
                FROM PRB_RFLV pr, MRK_Marker m
                WHERE pr._Marker_key = m._Marker_key AND m._Marker_Status_key IN (1,3) AND m._Marker_Type_key = 1''')
STATS[PLY_STRAINS] = ('Strains in MGI with detail pages',
        'SELECT COUNT(1) FROM %s' % StrainUtils.getStrainTempTable())

GROUPS[POLYMORPHISMS_MINI_HOME] = [ PLY_STRAINS, PLY_STRAINS_SNPS, PLY_REFSNPS, PLY_RFLP, PLY_PCR ]
GROUPS[POLYMORPHISMS_STATS_PAGE] = [ PLY_STRAINS, PLY_STRAINS_SNPS, PLY_REFSNPS, PLY_RFLP, PLY_PCR, PLY_GENES ]

###--- home page statistics ---###

HP_TOTAL_ALLELES = 'Total mutant alleles'
STATS[HP_TOTAL_ALLELES] = ('', 
        '''SELECT COUNT(1)
                FROM ALL_Allele
                WHERE isWildType = 0 AND _Allele_Type_key != 847130 AND _Allele_Status_key in (847114, 3983021)''')

GROUPS[HOME_PAGE] = [ MRK_GENES, MRK_GENES_DNA, MRK_GENES_GO, HP_TOTAL_ALLELES, GXD_ASSAYS, MP_DISEASES, 
        REFERENCES, PLY_REFSNPS ]

### Classes ###

class StatisticGatherer (Gatherer.Gatherer):
        def getCount(self, statisticName):
                if statisticName in self.counts:
                        return self.counts[statisticName]
                
                if statisticName in STATS:
                        cmd = STATS[statisticName][1]

                        # if an integer, it is a precomputed value
                        if type(cmd) == int:          
                                self.counts[statisticName] = cmd
                                return cmd
                                
                        # otherwise, it is a string SQL command with one column and one row
                        (cols, rows) = dbAgnostic.execute(cmd)
                        if rows:
                                self.counts[statisticName] = rows[0][0]
                                return rows[0][0]
                        
                return 0
                        
        def getTooltip(self, statisticName):
                if statisticName in STATS:
                        return STATS[statisticName][0]
                return ''
                
        def collateResults (self):
                allGroups = list(GROUPS.keys())
                allGroups.sort()
                
                self.finalColumns = [ 'statistic_key', 'name', 'tooltip',
                        'value', 'group_name', 'sequencenum', 'group_sequencenum' ]
                self.finalResults = []
                self.counts = {}                        # statistic name : integer count
                
                i = 0           # used for statistic key, group sequence num, and statistic sequence num
                
                for group in allGroups:
                        for statistic in GROUPS[group]:
                                i = i + 1
                                self.finalResults.append( [ i, statistic, self.getTooltip(statistic), 
                                        self.getCount(statistic), group, i, i ] )
                                        
                logger.debug('Computed %d statistics in %d groups' % (i, len(allGroups)))
                return

###--- globals ---###

cmds = [
        'select 0',                     # real queries are in global variables; this result can be ignored
        ]

fieldOrder = [ Gatherer.AUTO, 'statistic_key', 'name', 'tooltip',
                        'value', 'group_name', 'sequencenum', 'group_sequencenum' ]

filenamePrefix = 'statistic'

# global instance of a StatisticGatherer
gatherer = StatisticGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
