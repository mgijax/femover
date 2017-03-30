#!/bin/sh

#
# Post-processing step for setting the proper tab counts in the 'disease' table
#

. ./Configuration

psql -h ${PG_FE_DBSERVER} -U ${PG_FE_DBUSER} -d ${PG_FE_DBNAME} <<EOSQL

-- update genes_tab_count

WITH tmp_genes_counts AS (
SELECT gr.disease_key, count(gr.disease_key) AS genes_count
FROM disease_row_to_marker dm, disease_row r, disease_group_row gr, disease_group dg
        WHERE dm.disease_row_key = r.disease_row_key
        AND r.disease_row_key = gr.disease_row_key
        AND gr.disease_group_key = dg.disease_group_key
        AND dg.group_type in ('mouse only', 'human only', 'mouse and human')
        AND dm.is_causative = 1
        GROUP BY gr.disease_key
)
UPDATE disease
SET genes_tab_count = tmp_genes_counts.genes_count
FROM tmp_genes_counts
WHERE tmp_genes_counts.disease_key = disease.disease_key
;

select count(*) from disease where genes_tab_count > 0;

-- update models_tab_count

WITH tmp_models_counts AS (
SELECT gr.disease_key, count(gr.disease_key) AS models_count
FROM disease_row_to_model dm, disease_row r, disease_group_row gr, disease_group dg
        WHERE dm.disease_row_key = r.disease_row_key
        AND r.disease_row_key = gr.disease_row_key
        AND gr.disease_group_key = dg.disease_group_key
        AND dg.group_type in ('mouse only', 'human only', 'mouse and human')
        GROUP BY gr.disease_key
)
UPDATE disease
SET models_tab_count = tmp_models_counts.models_count
FROM tmp_models_counts
WHERE tmp_models_counts.disease_key = disease.disease_key
;

select count(*) from disease where models_tab_count > 0;

EOSQL

