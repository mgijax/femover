#!./python

# this TestData import brings in the constants used below (E.g. ID, DESCRIPTION,...)
from TestData import *

# some of these require complex data, so we can create some temp tables
TempTables = [
        
        "drop view if exists tmp_gxdview",
    "create view tmp_gxdview as select * from gxd_expression where isforgxd=1",
        # temp table mapping emaps_term and synonyms
        """
        DROP TABLE IF EXISTS tmp_emaps_ad
        """,
        """
        DROP TABLE IF EXISTS tmp_emaps_syn
        """,
        """
        select et._term_key _emaps_key,et.term emaps_term, vte._stage_key, vte._emapa_term_key
        into tmp_emaps_ad 
        from voc_term et 
        join voc_term_emaps vte on
                vte._term_key = et._term_key
        where et._vocab_key = 91 -- EMAPS
        """,
        """
        create index tmp_emaps_ad_ekey on tmp_emaps_ad (_emaps_key)
        """,
        """
        create index tmp_emaps_emapa_ekey on tmp_emaps_ad (_emapa_term_key)
        """,
        """
        create index tmp_emaps_stage_ekey on tmp_emaps_ad (_stage_key)
        """,
        # temp table combining emaps terms with their synonyms for searching
        """
        select t._term_key,accemapa.accid emapaid,t.term
        into tmp_emaps_syn
        from voc_term t join
                voc_term_emaps emaps on emaps._term_key=t._term_key join
                acc_accession accemapa on accemapa._object_key=emaps._emapa_term_key join 
                tmp_emaps_ad ead on ead._emaps_key=t._term_key
        where _vocab_key=91
                and accemapa._mgitype_key=13
        """,
        """
        insert into tmp_emaps_syn (_term_key,emapaid,term) 
        select distinct t._term_key, '',ts.synonym term
        from mgi_synonym ts join voc_term t on t._term_key=ts._object_key
        where _mgitype_key=13
        and t._vocab_key=91
        """,
        """
        create index tmp_emaps_syn_tkey on tmp_emaps_syn (_term_key)
        """,
        """
        create index tmp_emaps_syn_emapaid on tmp_emaps_syn (emapaid)
        """,
        """
        create index tmp_emaps_syn_term on tmp_emaps_syn (term)
        """
]

RemoveTempTables = [
        """
        DROP TABLE IF EXISTS tmp_emaps_ad
        """,
        """
        DROP TABLE IF EXISTS tmp_emaps_syn
        """
]

# The list of queries to generate GXD test data
Queries = [
###--- Assay type tests
{       ID:"RNaseProtectionResultCount",
        DESCRIPTION:"Assay Result count for RNaseProtection assay type",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview ge , gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and gt.assaytype='RNase protection'
        """
},
{       ID:"WesternBlotResultCount",
        DESCRIPTION:"Assay Result count for Western Blot assay type",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview ge , gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and gt.assaytype='Western blot'
        """
},
{       ID:"NorthernBlotResultCount",
        DESCRIPTION:"Assay Result count for Northern Blot assay type",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview ge , gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and gt.assaytype='Northern blot'
        """
},
{       ID:"NucleaseS1ResultCount",
        DESCRIPTION:"Assay Result count for Nuclease S1 assay type",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview ge , gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and gt.assaytype='Nuclease S1'
        """
},
{       ID:"RT-PCRResultCount",
        DESCRIPTION:"Assay Result count for RT-PCR assay type",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and gt.assaytype='RT-PCR'
        """
},
{       ID:"InSituKnockInResultCount",
        DESCRIPTION:"Assay Result count for In situ knockin assay type",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and gt.assaytype='In situ reporter (knock in)'
        """
},
{       ID:"ImmunohistochemistryResultCount",
        DESCRIPTION:"Assay Result count for Immunohistochemistry assay type",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and gt.assaytype='Immunohistochemistry'
        """
},
{       ID:"RNAInSituResultCount",
        DESCRIPTION:"Assay Result count for RNA in situ Blot assay type",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and gt.assaytype='RNA in situ'
        """
},

###--- Theiler Stage Tests
{       ID:"ts6ResultCount",
        DESCRIPTION:"Assay Result count for Theiler Stage 6",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview ge where ge._stage_key=6
        """
},
{       ID:"allResults",
        DESCRIPTION:"count of all gxd results",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview ge
        """
},
{       ID:"ts6or7ResultCount",
        DESCRIPTION:"Assay Result count for TS 6 or 7",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview ge where ge._stage_key in (6,7)
        """
},
{       ID:"ts28ResultCount",
        DESCRIPTION:"Assay Result count for TS 28",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview ge where ge._stage_key=28
        """
},
{       ID:"allGenes",
        DESCRIPTION:"Count of all GXD markers",
        SQLSTATEMENT:"""
        select count(distinct _marker_key) from tmp_gxdview ge
        """
},

###--- Age Tests
{       ID:"age2-3ResultCount",
        DESCRIPTION:"Assay Result count for ages 2.0,2.5,3.0",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview where (agemin <3.25 and agemax >=1.75)
        """
},
{       ID:"age5and6and7ResultCount",
        DESCRIPTION:"Assay Result count for ages 5.0,6.0,7.0",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview where ((agemin <5.25 and agemax >=4.75) or (agemin <6.25 and agemax >=5.75) or (agemin <7.25 and agemax >=6.75))
        """
},
{       ID:"age1and4ResultCount",
        DESCRIPTION:"Result count for ages 1.0 and 4.0",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview where ((agemin <1.25 and agemax >=0.75) or (agemin <4.25 and agemax >=3.75))
        """
},
]

# The following queries are all very large, but share similar syntax. Let's define a template to make this more readable.
EMAPID_ANATOMY_RESULTS_TEMPLATE_SQL = """
        WITH 
        struct AS (SELECT DISTINCT t._term_key FROM tmp_emaps_syn t
                WHERE t.emapaid = '%s'),
        child AS (SELECT DISTINCT clo._descendentobject_key FROM dag_closure clo 
                WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._ancestorobject_key = s._term_key) ),
        closure AS (SELECT * from struct UNION ALL SELECT * FROM child) 
        SELECT COUNT(distinct _expression_key) FROM gxd_expression e, tmp_emaps_ad tea, closure s 
        WHERE e._emapa_term_key = tea._emapa_term_key 
                and e._stage_key = tea._stage_key
                and tea._emaps_key= s._term_key AND e.isForGXD = 1
"""
EMAPID_ANATOMY_ASSAYS_TEMPLATE_SQL = """
        WITH 
        struct AS (SELECT DISTINCT t._term_key FROM tmp_emaps_syn t
                WHERE t.emapaid = '%s'),
        child AS (SELECT DISTINCT clo._descendentobject_key FROM dag_closure clo 
                WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._ancestorobject_key = s._term_key) ),
        closure AS (SELECT * from struct UNION ALL SELECT * FROM child) 
        SELECT COUNT(distinct _assay_key) FROM gxd_expression e, tmp_emaps_ad tea, closure s 
        WHERE e._emapa_term_key = tea._emapa_term_key 
                and e._stage_key = tea._stage_key
                and tea._emaps_key= s._term_key AND e.isForGXD = 1
"""
EMAPID_ANATOMY_GENES_TEMPLATE_SQL = """
        WITH 
        struct AS (SELECT DISTINCT t._term_key FROM tmp_emaps_syn t
                WHERE t.emapaid = '%s'),
        child AS (SELECT DISTINCT clo._descendentobject_key FROM dag_closure clo 
                WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._ancestorobject_key = s._term_key) ),
        closure AS (SELECT * from struct UNION ALL SELECT * FROM child) 
        SELECT COUNT(distinct _marker_key) FROM gxd_expression e, tmp_emaps_ad tea, closure s 
        WHERE e._emapa_term_key = tea._emapa_term_key
                and e._stage_key = tea._stage_key 
                and tea._emaps_key= s._term_key AND e.isForGXD = 1
"""

###--- Anatomy Tests
Queries.extend([
{       ID:"countFor4CellStageResults",
        DESCRIPTION:"find the results associated to term '4-cell stage embryo (EMAPA:31864)'",
        SQLSTATEMENT:EMAPID_ANATOMY_RESULTS_TEMPLATE_SQL%"EMAPA:31864"
},
{       ID:"countFor4CellStageAssays",
        DESCRIPTION:"find the assays associated to term '4-cell stage embryo (EMAPA:31864)'",
        SQLSTATEMENT:EMAPID_ANATOMY_ASSAYS_TEMPLATE_SQL%"EMAPA:31864"
},
{       ID:"countFor4CellStageGenes",
        DESCRIPTION:"find the genes associated to term '4-cell stage embryo (EMAPA:31864)'",
        SQLSTATEMENT:EMAPID_ANATOMY_GENES_TEMPLATE_SQL%"EMAPA:31864"
},
{       ID:"countForCorneaResults",
        DESCRIPTION:"find the results associated to term 'cornea (EMAPA:17161)'",
        SQLSTATEMENT:EMAPID_ANATOMY_RESULTS_TEMPLATE_SQL%"EMAPA:17161"
},
{       ID:"countForCorneaAssays",
        DESCRIPTION:"find the assays associated to term 'cornea (EMAPA:17161)'",
        SQLSTATEMENT:EMAPID_ANATOMY_ASSAYS_TEMPLATE_SQL%"EMAPA:17161"
},
{       ID:"countForCorneaGenes",
        DESCRIPTION:"find the genes associated to term 'cornea (EMAPA:17161)'",
        SQLSTATEMENT:EMAPID_ANATOMY_GENES_TEMPLATE_SQL%"EMAPA:17161"
},
])

###--- Multi Parameter Query Tests
Queries.extend([
{       ID:"resultCountShhTS21",
        DESCRIPTION:"Count of results for Shh and TS21",
        SQLSTATEMENT:"""
        select count(*) from tmp_gxdview ge, mrk_marker m 
        where m._marker_key=ge._marker_key
            and m.symbol='Shh'
            and ge._stage_key=21;
        """
},
{       ID:"resultCountPax*Immunohistochemistry",
        DESCRIPTION:"Count of results for pax* and Immunohistochemistry",
        SQLSTATEMENT:"""
        Select count(distinct ge._expression_key) from mrk_marker m, tmp_gxdview ge, mrk_label ml 
        where ge._assaytype_key=6
            and ge._marker_key=m._marker_key 
            and ml._marker_key=m._marker_key and ml.label ilike 'pax%'
            and ml.labeltypename in ('current name','current symbol','synonym','related synonym');
        """
},
{       ID:"geneCountPax*Immunohistochemistry",
        DESCRIPTION:"Count of genes for pax* and Immunohistochemistry",
        SQLSTATEMENT:"""
        Select count(distinct ge._marker_key) from mrk_marker m, tmp_gxdview ge, mrk_label ml 
        where ge._assaytype_key=6
            and ge._marker_key=m._marker_key 
            and ml._marker_key=m._marker_key and ml.label ilike 'pax%'
            and ml.labeltypename in ('current name','current symbol','synonym','related synonym');
        """
},
{       ID:"resultCountTS14NorthernBlot",
        DESCRIPTION:"Count of Results for TS14 and Northern Blot",
        SQLSTATEMENT:"""
        select count(distinct ge._expression_key) from tmp_gxdview ge
        where ge._stage_key=14
            and ge._assaytype_key=2;
        """
},
{       ID:"geneCountTS14NorthernBlot",
        DESCRIPTION:"Count of genes for TS14 and Northern Blot",
        SQLSTATEMENT:"""
        select count(distinct ge._marker_key) from tmp_gxdview ge
        where ge._stage_key=14
            and ge._assaytype_key=2;
        """
},
])

###--- Nomenclature query Tests
# these share a similar query syntax, let's make a template
NOMEN_TEMPLATE_SQL="""
        Select count(%s) from mrk_marker m, tmp_gxdview ge, mrk_label ml 
        where ge._marker_key=m._marker_key 
            and ml._marker_key=m._marker_key and ml.label ilike '%s'
            and ml.labeltypename in ('current name','current symbol','synonym','related synonym');
"""
Queries.extend([
{       ID:"pax6GeneCount",
        DESCRIPTION:"gene count for pax6",
        SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._marker_key","pax6%")
},
{       ID:"pax6AssayCount",
        DESCRIPTION:"assay count for pax6",
        SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._assay_key","pax6%")
},
{       ID:"pax6ResultCount",
        DESCRIPTION:"result count for pax6",
        SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._expression_key","pax6%")
},
{       ID:"pax6ImageCount",
        DESCRIPTION:"image count for pax6",
        SQLSTATEMENT:"""
        WITH
        pax6_markers AS(
                Select m._marker_key from mrk_marker m, mrk_label ml
                where ml._marker_key=m._marker_key and ml.label ilike 'pax6%'
                    and ml.labeltypename in ('current name','current symbol','synonym','related synonym')
        )
        select count(distinct _imagepane_key) pane_count
        from ((select distinct ga._imagepane_key,ga._marker_key 
                from gxd_assay ga,tmp_gxdview ge,pax6_markers pm,img_imagepane iip,img_image ii
                where ga._imagepane_key is not null and ge._assay_key=ge._assay_key
                        and pm._marker_key=ga._marker_key
                        and iip._imagepane_key=ga._imagepane_key
                        and ii._image_key=iip._image_key
                        and ii.xdim is not null) UNION 
            (select distinct ipv._imagepane_key,ga._marker_key 
                from gxd_isresultimage_view ipv,gxd_specimen sp,gxd_assay ga,
                        tmp_gxdview ge,pax6_markers pm,img_imagepane iip,img_image ii
                where ga._assay_key=sp._assay_key
                        and ipv._specimen_key=sp._specimen_key
                        and ga._assay_key=ge._assay_key
                        and pm._marker_key=ga._marker_key
                        and iip._imagepane_key=ipv._imagepane_key
                        and ii._image_key=iip._image_key
                        and ii.xdim is not null)) as foo;
        """
},
{       ID:"AfpAssayCount",
        DESCRIPTION:"assay count for afp",
        SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._assay_key","afp%")
},
{       ID:"shhGeneCount",
        DESCRIPTION:"gene count for shh",
        SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._marker_key","shh")
},
{       ID:"shhResultCount",
        DESCRIPTION:"result count for shh",
        SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._expression_key","shh")
},
{       ID:"gata1GeneCount",
        DESCRIPTION:"gene count for gata1",
        SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._marker_key","gata1")
},
{       ID:"gata1ResultCount",
        DESCRIPTION:"result count for gata1",
        SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._expression_key","gata1")
},
{       ID:"klfGeneCount",
        DESCRIPTION:"gene count for klf",
        SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._marker_key","klf%")
},
{       ID:"klfResultCount",
        DESCRIPTION:"result count for klf",
        SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._expression_key","klf%")
},
{       ID:"nomenNotSymbols",
        DESCRIPTION:"Finds all the marker symbols for the query 'not'",
        SQLSTATEMENT:"""
        Select distinct m.symbol from mrk_marker m, tmp_gxdview ge, mrk_label ml 
        where ge._marker_key=m._marker_key 
            and ml._marker_key=m._marker_key and (ml.label ~* '[^\\\\d\\\\w]not[^\\\\d\\\\w]' or ml.label ilike 'not')
            and ml.labeltypename in ('current name','current symbol','synonym','related synonym') order by m.symbol;

        """
},

])

###--- Mutants Query Tests
# NOTE: most of these queries return multi-valued (i.e. 2 column) results. These get translted into an html table in concordion
MUTANTS_BY_GENE_TEMPLATE_SQL = """
        Select acc.accid,count(distinct _expression_key) from tmp_gxdview ge, gxd_allelegenotype ag,mrk_label ml,acc_accession acc 
        where ge._genotype_key=ag._genotype_key 
                and ml._marker_key=ag._marker_key and ml.label = '%s' 
                and ml.labeltypename in ('current name','current symbol','synonym','related synonym') 
                and acc._object_key=ge._assay_key and acc._mgitype_key=8 and acc.preferred=1 
        group by acc.accid;
"""
Queries.extend([
{       ID:"resultCountMutantPax4GroupByAssayId",
        DESCRIPTION:"Gets result count for mutant Pax4 grouped by assayid",
        SQLSTATEMENT:MUTANTS_BY_GENE_TEMPLATE_SQL%"Pax4"
},
{       ID:"resultCountMutantPax1GroupByAssayId",
        DESCRIPTION:"Gets result count for mutant Pax1 grouped by assayid",
        SQLSTATEMENT:MUTANTS_BY_GENE_TEMPLATE_SQL%"Pax1"
},
{       ID:"resultCountMutantPax2GroupByAssayId",
        DESCRIPTION:"Gets result count for mutant Pax2 grouped by assayid",
        SQLSTATEMENT:MUTANTS_BY_GENE_TEMPLATE_SQL%"Pax2"
},
{       ID:"resultCountMutantAscl3GroupByAssayId",
        DESCRIPTION:"Gets result count for mutant Ascl3 grouped by assayid",
        SQLSTATEMENT:MUTANTS_BY_GENE_TEMPLATE_SQL%"Ascl3"
},
{       ID:"resultCountMutantAscl3NomenAscl3GroupByAssayId",
        DESCRIPTION:"Gets result count for mutant Ascl3, and Nomen Ascl3 grouped by assayid",
        SQLSTATEMENT:"""
        Select acc.accid,count(distinct _expression_key) from tmp_gxdview ge, gxd_allelegenotype ag,mrk_label ml,acc_accession acc,mrk_marker m 
        where ge._marker_key=m._marker_key and m.symbol='Ascl3'
                and ge._genotype_key=ag._genotype_key and ml._marker_key=ag._marker_key 
                and ml.label = 'Ascl3' and ml.labeltypename in ('current name','current symbol','synonym','related synonym') 
                and acc._object_key=ge._assay_key and acc._mgitype_key=8 and acc.preferred=1 
        group by acc.accid;
        """
},
{       ID:"resultCountByMutantBetaCateninGroupByAssayId",
        DESCRIPTION:"Gets result count for 'beta catenin' grouped by assayid",
        SQLSTATEMENT:"""
        Select acc.accid,count(distinct _expression_key) 
        from tmp_gxdview ge, gxd_allelegenotype ag,mrk_label ml,acc_accession acc 
        where ge._genotype_key=ag._genotype_key 
                and ml._marker_key=ag._marker_key 
                and ml.label @@ 'beta & catenin'
                and ml.labeltypename in ('current name','current symbol','synonym','related synonym') 
                and acc._object_key=ge._assay_key 
                and acc._mgitype_key=8 
                and acc.preferred=1 group by acc.accid;
        """
},
{       ID:"resultCountByMutantKrox-20GroupByAssayId",
        DESCRIPTION:"Gets result count for 'Krox-20' grouped by assayid",
        SQLSTATEMENT:"""
        Select acc.accid,count(distinct _expression_key) 
        from tmp_gxdview ge, gxd_allelegenotype ag,mrk_label ml,acc_accession acc 
        where ge._genotype_key=ag._genotype_key 
                and ml._marker_key=ag._marker_key 
                and ml.label @@ 'Krox-20'
                and ml.labeltypename in ('current name','current symbol','synonym','related synonym') 
                and acc._object_key=ge._assay_key 
                and acc._mgitype_key=8 
                and acc.preferred=1 group by acc.accid;
        """
},
{       ID:"wildtypeSpecimenCount",
        DESCRIPTION:"non-insitu reporters specimens that are wild type",
        SQLSTATEMENT:"""
        select count(distinct e._assay_key) from tmp_gxdview e, GXD_Specimen g 
        where e._assaytype_key != 9 
                and e._assay_key = g._assay_key 
                and not exists (select 1 from GXD_AlleleGenotype ag where g._Genotype_key = ag._Genotype_key)
        """
},
{       ID:"wildtypeGelLaneCount",
        DESCRIPTION:"non-insitu reporters gel lanes that are wild type",
        SQLSTATEMENT:"""
        select count(distinct e._assay_key) from tmp_gxdview e, GXD_GelLane g, VOC_Term t
        where e._assaytype_key != 9 
                and e._assay_key = g._assay_key 
                and g._gelcontrol_key = t._term_key and t._vocab_key = 154 and t.term = 'No'
                and not exists (select 1 from GXD_AlleleGenotype ag where g._Genotype_key = ag._Genotype_key)
        """
},
{       ID:"wildtypeInSituReportersCount",
        DESCRIPTION:"insitu reporters that are wild type",
        SQLSTATEMENT:"""
        select ag._genotype_key into temporary tmp_genotype 
                from GXD_AllelePair ag group by ag._Genotype_key 
                having count(*) = 1;create index idx1 on tmp_genotype(_genotype_key);
        select count(distinct e._assay_key )from tmp_gxdview e, tmp_genotype ag, GXD_AlleleGenotype agg, ALL_Allele a 
        where e._assaytype_key = 9 
                and e._genotype_key = ag._genotype_key 
                and ag._genotype_key = agg._genotype_key 
                and agg._marker_key = e._marker_key 
                and agg._allele_key = a._allele_key 
                and a.iswildtype = 1;
        """
},
{       ID:"allWildTypeCount",
        DESCRIPTION:"count of all the new wild type results",
        SQLSTATEMENT:"""
        WITH tmp_genotype AS (select ag._genotype_key from GXD_AllelePair ag group by ag._Genotype_key having count(*) = 1),
        new_wildtypes AS (select distinct e._expression_key from tmp_gxdview e, tmp_genotype ag, GXD_AlleleGenotype agg, ALL_Allele a 
                where e._assaytype_key = 9 
                        and e._genotype_key = ag._genotype_key 
                        and ag._genotype_key = agg._genotype_key 
                        and agg._marker_key = e._marker_key 
                        and agg._allele_key = a._allele_key 
                        and a.iswildtype = 1),
        old_wildtypes AS (select distinct ge._expression_key from tmp_gxdview ge 
                where ge.isforgxd=1 
                        and ge._assaytype_key!=9 
                        and not exists (select 1 from gxd_allelegenotype ag where ag._genotype_key=ge._genotype_key))
        select count(*) from (select * from old_wildtypes UNION select * from new_wildtypes) as all_wildtypes;
        """
},
{       ID:"wildtypeCountForAscl3GroupByAssayId",
        DESCRIPTION:"count of all wildtype results for Ascl3 grouped by assay id",
        SQLSTATEMENT:"""
        WITH tmp_genotype AS (select ag._genotype_key from GXD_AllelePair ag group by ag._Genotype_key having count(*) = 1),
        new_wildtypes AS (select e.* from tmp_gxdview e, tmp_genotype ag, GXD_AlleleGenotype agg, ALL_Allele a 
                where e._assaytype_key = 9 
                and e._genotype_key = ag._genotype_key 
                and ag._genotype_key = agg._genotype_key 
                and agg._marker_key = e._marker_key 
                and agg._allele_key = a._allele_key 
                and a.iswildtype = 1),
        old_wildtypes AS (select ge.* from tmp_gxdview ge 
                where ge.isforgxd=1 
                        and ge._assaytype_key!=9 
                        and not exists (select 1 from gxd_allelegenotype ag where ag._genotype_key=ge._genotype_key))
        select acc.accid,count(distinct _expression_key) from mrk_marker m,acc_accession acc,
                (select * from old_wildtypes UNION select * from new_wildtypes) as all_wildtypes 
        where m._marker_key=all_wildtypes._marker_key 
                and acc._object_key=all_wildtypes._assay_key
                and acc._mgitype_key=8 
                and acc.preferred=1 
                and m.symbol='Ascl3' 
        group by acc.accid;
        """
},
])
