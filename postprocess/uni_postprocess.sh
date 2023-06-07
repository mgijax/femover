#!/bin/sh

#
# Post-processing step for building uni_all_genes_tissues table.
# Combines and boils down classical and RNAseq results to unique marker,tissue
# pairs, where there is a positive expression result.
#

. ../Configuration


psql -h ${PG_DBSERVER} -U ${PG_DBUSER} -d ${PG_DBNAME} <<EOSQL1

DROP TABLE IF EXISTS uni_keystone;

EOSQL1

##

psql -h ${PG_FE_DBSERVER} -U ${PG_FE_DBUSER} -d ${PG_FE_DBNAME} <<EOSQL

DROP TABLE IF EXISTS uni_all_genes_tissues;

SELECT distinct 
    sm.marker_key,
    te.term_key as structure_key 
INTO uni_all_genes_tissues
FROM expression_ht_consolidated_sample_measurement sm, 
    expression_ht_consolidated_sample s, 
    term_emap te 
WHERE sm.level != 'Below Cutoff' 
AND sm.consolidated_sample_key = s.consolidated_sample_key 
AND s.emapa_key = te.emapa_term_key 
AND cast(s.theiler_stage as int) = te.stage 

UNION 

SELECT distinct  
    marker_key as subject_key, 
    structure_key 
FROM expression_result_summary 
WHERE is_expressed = 'Yes' 
;

GRANT SELECT ON uni_all_genes_tissues TO PUBLIC;

EOSQL

