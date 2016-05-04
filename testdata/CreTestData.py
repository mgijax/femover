#!/usr/local/bin/python

# this TestData import brings in the constants used below (E.g. ID, DESCRIPTION,...)
from TestData import *
import GXDTestData

TempTables = GXDTestData.TempTables
RemoveTempTables = GXDTestData.RemoveTempTables

# NOTE: this uses temp tables defined in GXDTestData
TEMPLATE_CRE_STRUCTURE_SQL = """
        WITH 
        struct AS (SELECT DISTINCT t._term_key FROM tmp_emaps_syn t
                WHERE term @@ array_to_string(string_to_array('%s', ' '), ' & ')),
        child AS (SELECT DISTINCT clo._descendentobject_key FROM dag_closure clo 
                WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._ancestorobject_key = s._term_key) ),
        closure AS (SELECT * from struct UNION ALL SELECT * FROM child) 
        SELECT count(distinct cre._allele_key) FROM gxd_expression e,gxd_allelegenotype gag,all_cre_cache cre, closure s,tmp_emaps_ad tea
	WHERE e._emapa_term_key = tea._emapa_term_key
	    and e._stage_key = tea._stage_key
	    and tea._emaps_key=s._term_key 
	    AND e.isrecombinase = 1 
	    AND gag._genotype_key=e._genotype_key 
	    AND gag._allele_key=cre._allele_key;
"""
# The list of queries to generate GXD test data
Queries = [
{	ID:"creDorsalMesocardiumAlleleCount",
	DESCRIPTION:"Count of cre alleles associated with dorsal mesocardium",
	SQLSTATEMENT:TEMPLATE_CRE_STRUCTURE_SQL%"dorsal mesocardium"
},
{	ID:"creEsophagusAlleleCount",
	DESCRIPTION:"Count of cre alleles associated with esophagus",
	SQLSTATEMENT:TEMPLATE_CRE_STRUCTURE_SQL%"esophagus"
},
{	ID:"creBronchusEpitheliumAlleleCount",
	DESCRIPTION:"Count of cre alleles associated with bronchus epithelium",
	SQLSTATEMENT:TEMPLATE_CRE_STRUCTURE_SQL%"bronchus epithelium"
},
{	ID:"creBronchialEpitheliumAlleleCount",
	DESCRIPTION:"Count of cre alleles associated with bronchial epithelium",
	SQLSTATEMENT:TEMPLATE_CRE_STRUCTURE_SQL%"bronchial epithelium"
},
{	ID:"creUterusAlleleCount",
	DESCRIPTION:"Count of cre alleles associated with uterus",
	SQLSTATEMENT:TEMPLATE_CRE_STRUCTURE_SQL%"uterus"
},
# copy above lines to make more tests
]

