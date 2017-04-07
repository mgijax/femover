#!/bin/sh

#
# Post-processing step for setting the proper tab counts in the 'disease' table
#

. ../Configuration

psql -h ${PG_FE_DBSERVER} -U ${PG_FE_DBUSER} -d ${PG_FE_DBNAME} <<EOSQL

-- update disease.genes_tab_count

SELECT gr.disease_key
INTO TEMP tmp_genes
FROM disease_row_to_marker dm, disease_row r, disease_group_row gr, disease_group dg
WHERE dm.disease_row_key = r.disease_row_key
        AND r.disease_row_key = gr.disease_row_key
        AND gr.disease_group_key = dg.disease_group_key
        AND dg.group_type in ('mouse only', 'human only', 'mouse and human')
        AND dm.is_causative = 1
        GROUP BY gr.disease_key, dm.marker_key
;
WITH tmp_genes_counts AS (
SELECT disease_key, count(disease_key) as genes_count
FROM tmp_genes
GROUP BY disease_key
)
UPDATE disease
SET genes_tab_count = tmp_genes_counts.genes_count
FROM tmp_genes_counts
WHERE tmp_genes_counts.disease_key = disease.disease_key
;

select count(*) from disease where genes_tab_count > 0;

--
-- update disease.models_tab_count
-- generate counts by unique:
-- 	disease (disease_key)
--	genotype (genotype.background_strain) 
--	allele (genotype.combination_1)
--

SELECT gr.disease_key
INTO TEMP tmp_models
FROM disease_row_to_model drm, disease_row r, disease_group_row gr, disease_group dg, disease_model dm, genotype g        
WHERE drm.disease_row_key = r.disease_row_key
        AND r.disease_row_key = gr.disease_row_key
        AND gr.disease_group_key = dg.disease_group_key
        AND dg.group_type in ('mouse only', 'human only', 'mouse and human', 'additional', 'non-genes')
        AND drm.disease_model_key = dm.disease_model_key
        AND dm.is_not_model = 0
        AND dm.genotype_key = g.genotype_key
        AND g.is_disease_model = 1
        GROUP BY gr.disease_key, g.background_strain, g.combination_1
;
WITH tmp_models_counts AS (
SELECT disease_key, count(disease_key) as models_count
FROM tmp_models
GROUP BY disease_key
)
UPDATE disease
SET models_tab_count = tmp_models_counts.models_count
FROM tmp_models_counts
WHERE tmp_models_counts.disease_key = disease.disease_key
;

select count(*) from disease where models_tab_count > 0;

EOSQL

