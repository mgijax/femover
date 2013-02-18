#!/usr/local/bin/python

# this TestData import brings in the constants used below (E.g. ID, DESCRIPTION,...)
from TestData import *

# The list of queries to generate GXD test data
Queries = [
###--- Assay type tests
{	ID:"RNaseProtectionResultCount",
	DESCRIPTION:"Assay Result count for RNaseProtection assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='RNase protection'
	"""
},
{	ID:"WesternBlotResultCount",
	DESCRIPTION:"Assay Result count for Western Blot assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='Western blot'
	"""
},
{	ID:"NorthernBlotResultCount",
	DESCRIPTION:"Assay Result count for Northern Blot assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='Northern blot'
	"""
},
{	ID:"NucleaseS1ResultCount",
	DESCRIPTION:"Assay Result count for Nuclease S1 assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='Nuclease S1'
	"""
},
{	ID:"RT-PCRResultCount",
	DESCRIPTION:"Assay Result count for RT-PCR assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='RT-PCR'
	"""
},
{	ID:"InSituKnockInResultCount",
	DESCRIPTION:"Assay Result count for In situ knockin assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='In situ reporter (knock in)'
	"""
},
{	ID:"ImmunohistochemistryResultCount",
	DESCRIPTION:"Assay Result count for Immunohistochemistry assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='Immunohistochemistry'
	"""
},
{	ID:"RNAInSituResultCount",
	DESCRIPTION:"Assay Result count for RNA in situ Blot assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='RNA in situ'
	"""
},

###--- Theiler Stage Tests
{	ID:"ts6ResultCount",
	DESCRIPTION:"Assay Result count for Theiler Stage 6",
	SQLSTATEMENT:"""
	select count(*) from gxd_expression ge, gxd_Structure gs where ge._structure_key=gs._structure_key and ge.isforgxd=1 and gs._stage_key=6
	"""
},
{	ID:"allResults",
	DESCRIPTION:"count of all gxd results",
	SQLSTATEMENT:"""
	select count(*) from gxd_expression ge, gxd_Structure gs where ge._structure_key=gs._structure_key and ge.isforgxd=1
	"""
},
{	ID:"ts6or7ResultCount",
	DESCRIPTION:"Assay Result count for TS 6 or 7",
	SQLSTATEMENT:"""
	select count(*) from gxd_expression ge, gxd_Structure gs where ge._structure_key=gs._structure_key and ge.isforgxd=1 and gs._stage_key in (6,7)
	"""
},
{	ID:"ts28ResultCount",
	DESCRIPTION:"Assay Result count for TS 28",
	SQLSTATEMENT:"""
	select count(*) from gxd_expression ge, gxd_Structure gs where ge._structure_key=gs._structure_key and ge.isforgxd=1 and gs._stage_key=28
	"""
},
{	ID:"allGenes",
	DESCRIPTION:"Count of all GXD markers",
	SQLSTATEMENT:"""
	select count(distinct _marker_key) from gxd_expression ge, gxd_Structure gs where ge._structure_key=gs._structure_key and ge.isforgxd=1
	"""
},

###--- Age Tests
{	ID:"age2-3ResultCount",
	DESCRIPTION:"Assay Result count for ages 2.0,2.5,3.0",
	SQLSTATEMENT:"""
	select count(*) from gxd_Expression where isforgxd=1 and (agemin <3.25 and agemax >=1.75)
	"""
},
{	ID:"age5and6and7ResultCount",
	DESCRIPTION:"Assay Result count for ages 5.0,6.0,7.0",
	SQLSTATEMENT:"""
	select count(*) from gxd_Expression where isforgxd=1 and ((agemin <5.25 and agemax >=4.75) or (agemin <6.25 and agemax >=5.75) or (agemin <7.25 and agemax >=6.75))
	"""
},
{	ID:"age1and4ResultCount",
	DESCRIPTION:"Result count for ages 1.0 and 4.0",
	SQLSTATEMENT:"""
	select count(*) from gxd_Expression where isforgxd=1 and ((agemin <1.25 and agemax >=0.75) or (agemin <4.25 and agemax >=3.75))
	"""
},
]

# The following queries are all very large, but share similar syntax. Let's define a template to make this more readable.
SINGLEWORD_ANATOMY_RESULTS_TEMPLATE_SQL = """
	WITH 
        struct AS (SELECT DISTINCT _Structure_key FROM gxd_structurename 
		WHERE structure ILIKE '%%%s%%'),
        syn AS (SELECT DISTINCT clo._Descendent_key FROM gxd_structureclosure clo 
                WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._Structure_key = s._Structure_key) 
                AND NOT EXISTS (SELECT 1 FROM struct s WHERE clo._Descendent_key = s._Structure_key)),
        closure AS (SELECT * from struct UNION ALL SELECT * FROM syn) 
        SELECT COUNT(*) FROM gxd_expression e, closure s WHERE e._Structure_key = s._Structure_key AND e.isForGXD = 1
"""
SINGLEWORD_ANATOMY_ASSAYS_TEMPLATE_SQL = """
	WITH 
        struct AS (SELECT DISTINCT _Structure_key FROM gxd_structurename 
		WHERE structure ILIKE '%%%s%%'),
        syn AS (SELECT DISTINCT clo._Descendent_key FROM gxd_structureclosure clo 
                WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._Structure_key = s._Structure_key) 
                AND NOT EXISTS (SELECT 1 FROM struct s WHERE clo._Descendent_key = s._Structure_key)),
        closure AS (SELECT * from struct UNION ALL SELECT * FROM syn) 
        SELECT COUNT(distinct _assay_key) FROM gxd_expression e, closure s WHERE e._Structure_key = s._Structure_key AND e.isForGXD = 1
"""
SINGLEWORD_ANATOMY_GENES_TEMPLATE_SQL = """
	WITH 
        struct AS (SELECT DISTINCT _Structure_key FROM gxd_structurename 
		WHERE structure ILIKE '%%%s%%'),
        syn AS (SELECT DISTINCT clo._Descendent_key FROM gxd_structureclosure clo 
                WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._Structure_key = s._Structure_key) 
                AND NOT EXISTS (SELECT 1 FROM struct s WHERE clo._Descendent_key = s._Structure_key)),
        closure AS (SELECT * from struct UNION ALL SELECT * FROM syn) 
        SELECT COUNT(distinct _marker_key) FROM gxd_expression e, closure s WHERE e._Structure_key = s._Structure_key AND e.isForGXD = 1
"""
MULTIWORD_ANATOMY_RESULTS_TEMPLATE_SQL = """
	WITH 
	struct AS (SELECT DISTINCT _Structure_key FROM gxd_structurename 
		WHERE structure @@ array_to_string(string_to_array('%s', ' '), ' & ')),
	syn AS (SELECT DISTINCT clo._Descendent_key FROM gxd_structureclosure clo 
		WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._Structure_key = s._Structure_key) 
		AND NOT EXISTS (SELECT 1 FROM struct s WHERE clo._Descendent_key = s._Structure_key)),
	closure AS (SELECT * from struct UNION ALL SELECT * FROM syn) 
	SELECT COUNT(*) FROM gxd_expression e, closure s WHERE e._Structure_key = s._Structure_key AND e.isForGXD = 1
"""
###--- Anatomy Tests
Queries.extend([
{	ID:"structureOneWord",
	DESCRIPTION:"Finds a structure with only one word",
	SQLSTATEMENT:"""
	select sn.structure from  gxd_structure s, gxd_structurename sn 
		where s._structurename_key=sn._structurename_key 
		and sn.structure not like '% %' and sn.structure not like '%-%' and sn.structure not like '%/%' and sn.structure not like '%(%' 
		and exists (select 1 from gxd_expression ge where isforgxd=1 and ge._structure_key=s._structure_key) order by random() limit 1;
	"""
},
{	ID:"structureOneWordResultCount",
	DESCRIPTION:"Find count of results for the above structure",
	SQLSTATEMENT:SINGLEWORD_ANATOMY_RESULTS_TEMPLATE_SQL%"${structureOneWord}"
},
{	ID:"structureOneWordGeneCount",
	DESCRIPTION:"finds the count of genes for above structure",
	SQLSTATEMENT:"""
	WITH 
	struct AS (SELECT DISTINCT _Structure_key FROM gxd_structurename WHERE structure ILIKE '%${structureOneWord}%'),
	syn AS (SELECT DISTINCT clo._Descendent_key FROM gxd_structureclosure clo 
		WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._Structure_key = s._Structure_key) 
		AND NOT EXISTS (SELECT 1 FROM struct s WHERE clo._Descendent_key = s._Structure_key)),
	closure AS (SELECT * from struct UNION ALL SELECT * FROM syn) 
	SELECT COUNT(distinct _marker_key) FROM gxd_expression e, closure s WHERE e._Structure_key = s._Structure_key AND e.isForGXD = 1
	"""
},
{	ID:"countFor4CellStageResults",
	DESCRIPTION:"find the results associated to term '4-cell stage'",
	SQLSTATEMENT:MULTIWORD_ANATOMY_RESULTS_TEMPLATE_SQL%"4-cell stage"
},
{	ID:"countFor4CellStageAssays",
	DESCRIPTION:"find the assays associated to term '4-cell stage'",
	SQLSTATEMENT:"""
	WITH 
	struct AS (SELECT DISTINCT _Structure_key FROM gxd_structurename WHERE structure @@ array_to_string(string_to_array('4-cell stage', ' '), ' & ')),
	syn AS (SELECT DISTINCT clo._Descendent_key FROM gxd_structureclosure clo WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._Structure_key = s._Structure_key) AND NOT EXISTS (SELECT 1 FROM struct s WHERE clo._Descendent_key = s._Structure_key)),
	closure AS (SELECT * from struct UNION ALL SELECT * FROM syn) 
	SELECT COUNT(distinct _assay_key) FROM gxd_expression e, closure s WHERE e._Structure_key = s._Structure_key AND e.isForGXD = 1
	"""
},
{	ID:"countFor4CellStageGenes",
	DESCRIPTION:"find the genes associated to term '4-cell stage'",
	SQLSTATEMENT:"""
	WITH 
	struct AS (SELECT DISTINCT _Structure_key FROM gxd_structurename WHERE structure @@ array_to_string(string_to_array('4-cell stage', ' '), ' & ')),
	syn AS (SELECT DISTINCT clo._Descendent_key FROM gxd_structureclosure clo WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._Structure_key = s._Structure_key) AND NOT EXISTS (SELECT 1 FROM struct s WHERE clo._Descendent_key = s._Structure_key)),
	closure AS (SELECT * from struct UNION ALL SELECT * FROM syn) 
	SELECT COUNT(distinct _marker_key) FROM gxd_expression e, closure s WHERE e._Structure_key = s._Structure_key AND e.isForGXD = 1
	"""
},
{	ID:"countFor4-8CellStageEmbryoResults",
	DESCRIPTION:"find the results associated to term '4-8 cell stage embryo'",
	SQLSTATEMENT:MULTIWORD_ANATOMY_RESULTS_TEMPLATE_SQL%"4-8cell stage embryo"
},
{	ID:"countFor3rdBranchialArchResults",
	DESCRIPTION:"find the results associated to term '3rd branchial arch'",
	SQLSTATEMENT:MULTIWORD_ANATOMY_RESULTS_TEMPLATE_SQL%"3rd branchial arch"
},
{	ID:"countFor2ndArchBranchialGrooveResults",
	DESCRIPTION:"find the results associated to term '2nd arch branchial groove'",
	SQLSTATEMENT:MULTIWORD_ANATOMY_RESULTS_TEMPLATE_SQL%"2nd arch branchial groove"
},
{	ID:"countForDigit1MetacarpusResults",
	DESCRIPTION:"find the results associated to term 'digit 1 metacarpus'",
	SQLSTATEMENT:MULTIWORD_ANATOMY_RESULTS_TEMPLATE_SQL%"digit 1 metacarpus"
},
{	ID:"countForCorneaResults",
	DESCRIPTION:"find the results associated to term 'cornea'",
	SQLSTATEMENT:SINGLEWORD_ANATOMY_RESULTS_TEMPLATE_SQL%"cornea"
},
{	ID:"countForCorneaAssays",
	DESCRIPTION:"find the assays associated to term 'cornea'",
	SQLSTATEMENT:SINGLEWORD_ANATOMY_ASSAYS_TEMPLATE_SQL%"cornea"
},
{	ID:"countForCorneaGenes",
	DESCRIPTION:"find the genes associated to term 'cornea'",
	SQLSTATEMENT:SINGLEWORD_ANATOMY_GENES_TEMPLATE_SQL%"cornea"
},
{	ID:"countForBlastocystResults",
	DESCRIPTION:"find the results associated to term 'blastocyst'",
	SQLSTATEMENT:SINGLEWORD_ANATOMY_RESULTS_TEMPLATE_SQL%"blastocyst"
},
{	ID:"countForBlastocystAssays",
	DESCRIPTION:"find the assays associated to term 'blastocyst'",
	SQLSTATEMENT:SINGLEWORD_ANATOMY_ASSAYS_TEMPLATE_SQL%"blastocyst"
},
{	ID:"countForBlastocystGenes",
	DESCRIPTION:"find the genes associated to term 'blastocyst'",
	SQLSTATEMENT:SINGLEWORD_ANATOMY_GENES_TEMPLATE_SQL%"blastocyst"
},
{	ID:"countForAbdominalMuscleResults",
	DESCRIPTION:"find the count of results associated to the term which has the synonym 'abdominal muscle', and descendants",
	SQLSTATEMENT:MULTIWORD_ANATOMY_RESULTS_TEMPLATE_SQL%"abdominal muscle"
},
{	ID:"countForMaturingNephronResults",
	DESCRIPTION:"find the count of results associated to terms containing 'maturing nephron', and their children",
	SQLSTATEMENT:"""
	WITH 
	struct AS (SELECT DISTINCT _Structure_key FROM gxd_structurename 
		WHERE structure ilike '%maturing nephron%'),
	syn AS (SELECT DISTINCT clo._Descendent_key FROM gxd_structureclosure clo 
		WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._Structure_key = s._Structure_key) 
		AND NOT EXISTS (SELECT 1 FROM struct s WHERE clo._Descendent_key = s._Structure_key)),
	closure AS (SELECT * from struct UNION ALL SELECT * FROM syn) 
	SELECT COUNT(*) FROM gxd_expression e, closure s WHERE e._Structure_key = s._Structure_key AND e.isForGXD = 1
	"""
},
{	ID:"countForMatureNephronResults",
	DESCRIPTION:"find the count of results associated to terms containing 'mature nephron', and their children",
	SQLSTATEMENT:MULTIWORD_ANATOMY_RESULTS_TEMPLATE_SQL%"mature nephron"
	SQLSTATEMENT:"""
	WITH 
	struct AS (SELECT DISTINCT _Structure_key FROM gxd_structurename 
		WHERE structure ilike '%mature nephron%'),
	syn AS (SELECT DISTINCT clo._Descendent_key FROM gxd_structureclosure clo 
		WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._Structure_key = s._Structure_key) 
		AND NOT EXISTS (SELECT 1 FROM struct s WHERE clo._Descendent_key = s._Structure_key)),
	closure AS (SELECT * from struct UNION ALL SELECT * FROM syn) 
	SELECT COUNT(*) FROM gxd_expression e, closure s WHERE e._Structure_key = s._Structure_key AND e.isForGXD = 1
	"""
},
])

###--- Multi Parameter Query Tests
Queries.extend([
{	ID:"resultCountShhTS21",
	DESCRIPTION:"Count of results for Shh and TS21",
	SQLSTATEMENT:"""
	select count(*) from gxd_expression ge, mrk_marker m, gxd_structure s 
	where m._marker_key=ge._marker_key
	    and s._structure_key=ge._structure_key
	    and m.symbol='Shh'
	    and s._stage_key=21
	    and ge.isforgxd=1;
	"""
},
{	ID:"resultCountPax*Immunohistochemistry",
	DESCRIPTION:"Count of results for pax* and Immunohistochemistry",
	SQLSTATEMENT:"""
	Select count(distinct ge._expression_key) from mrk_marker m, gxd_Expression ge, mrk_label ml 
	where ge.isforgxd=1 
	    and ge._assaytype_key=6
	    and ge._marker_key=m._marker_key 
	    and ml._marker_key=m._marker_key and ml.label ilike 'pax%'
	    and ml.labeltypename in ('current name','current symbol','synonym','related synonym');
	"""
},
{	ID:"geneCountPax*Immunohistochemistry",
	DESCRIPTION:"Count of genes for pax* and Immunohistochemistry",
	SQLSTATEMENT:"""
	Select count(distinct ge._marker_key) from mrk_marker m, gxd_Expression ge, mrk_label ml 
	where ge.isforgxd=1 
	    and ge._assaytype_key=6
	    and ge._marker_key=m._marker_key 
	    and ml._marker_key=m._marker_key and ml.label ilike 'pax%'
	    and ml.labeltypename in ('current name','current symbol','synonym','related synonym');
	"""
},
{	ID:"resultCountTS14NorthernBlot",
	DESCRIPTION:"Count of Results for TS14 and Northern Blot",
	SQLSTATEMENT:"""
	select count(distinct ge._expression_key) from gxd_expression ge, gxd_structure s
	where ge.isforgxd=1
	    and ge._structure_key=s._structure_key
	    and s._stage_key=14
	    and ge._assaytype_key=2;
	"""
},
{	ID:"geneCountTS14NorthernBlot",
	DESCRIPTION:"Count of genes for TS14 and Northern Blot",
	SQLSTATEMENT:"""
	select count(distinct ge._marker_key) from gxd_expression ge, gxd_structure s
	where ge.isforgxd=1
	    and ge._structure_key=s._structure_key
	    and s._stage_key=14
	    and ge._assaytype_key=2;
	"""
},
])

###--- Nomenclature query Tests
# these share a similar query syntax, let's make a template
NOMEN_TEMPLATE_SQL="""
	Select count(%s) from mrk_marker m, gxd_Expression ge, mrk_label ml 
	where ge.isforgxd=1 
	    and ge._marker_key=m._marker_key 
	    and ml._marker_key=m._marker_key and ml.label ilike '%s'
	    and ml.labeltypename in ('current name','current symbol','synonym','related synonym');
"""
Queries.extend([
{	ID:"pax6GeneCount",
	DESCRIPTION:"gene count for pax6",
	SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._marker_key","pax6%")
},
{	ID:"pax6AssayCount",
	DESCRIPTION:"gene count for pax6",
	SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._assay_key","pax6%")
},
{	ID:"pax6ResultCount",
	DESCRIPTION:"result count for pax6",
	SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._expression_key","pax6%")
},
{	ID:"shhGeneCount",
	DESCRIPTION:"gene count for shh",
	SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._marker_key","shh")
},
{	ID:"shhResultCount",
	DESCRIPTION:"result count for shh",
	SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._expression_key","shh")
},
{	ID:"gata1GeneCount",
	DESCRIPTION:"gene count for gata1",
	SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._marker_key","gata1")
},
{	ID:"gata1ResultCount",
	DESCRIPTION:"result count for gata1",
	SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._expression_key","gata1")
},
{	ID:"klfGeneCount",
	DESCRIPTION:"gene count for klf",
	SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._marker_key","klf%")
},
{	ID:"klfResultCount",
	DESCRIPTION:"result count for klf",
	SQLSTATEMENT:NOMEN_TEMPLATE_SQL%("distinct ge._expression_key","klf%")
},
{	ID:"nomenNotSymbols",
	DESCRIPTION:"Finds all the marker symbols for the query 'not'",
	SQLSTATEMENT:"""
	Select distinct m.symbol from mrk_marker m, gxd_Expression ge, mrk_label ml 
	where ge.isforgxd=1 
	    and ge._marker_key=m._marker_key 
	    and ml._marker_key=m._marker_key and (ml.label ~* '[^\\\\d\\\\w]not[^\\\\d\\\\w]' or ml.label ilike 'not')
	    and ml.labeltypename in ('current name','current symbol','synonym','related synonym') order by m.symbol;

	"""
},

])
