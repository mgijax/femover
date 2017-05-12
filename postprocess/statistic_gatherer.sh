#!/bin/sh

#
# Post-processing step for updating two disease-related statistics in the 'statistic' table
#

. ../Configuration

psql -h ${PG_FE_DBSERVER} -U ${PG_FE_DBUSER} -d ${PG_FE_DBNAME} <<EOSQL

-- update count of "Mouse genotypes modeling human diseases" using model_tab_count from DOID:4

update statistic
set value = (select models_tab_count from disease where primary_id = 'DOID:4')
where name = 'Mouse genotypes modeling human diseases'
;

-- count of diseases with direct annotations to models

update statistic
set value = (SELECT count(distinct gr.annotated_disease)
	FROM disease_row_to_model drm, disease_row r, disease_group_row gr, disease_group dg, disease_model dm, genotype g        
	WHERE drm.disease_row_key = r.disease_row_key
        AND r.disease_row_key = gr.disease_row_key
        AND gr.disease_group_key = dg.disease_group_key
        AND dg.group_type in ('mouse only', 'human only', 'mouse and human', 'additional', 'non-genes')
        AND drm.disease_model_key = dm.disease_model_key
        AND dm.is_not_model = 0
        AND dm.genotype_key = g.genotype_key
        AND g.is_disease_model = 1)
where name = 'Human diseases with one or more mouse models'
;

EOSQL

