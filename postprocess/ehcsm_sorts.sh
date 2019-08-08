#!/bin/sh

#
# Post-processing step for precomputing sort values for RNA-Seq experiments' consolidated sample measurements.
#

. ../Configuration

echo "Beginning at " `uptime | sed 's/ //' | awk '{print $1}'`

#../postprocess/expression_ht_consolidated_sample_measurement_sorts.py
../postprocess/ehcsm_sorts.py key ${DATA_DIR}/ehcsm_key.txt &
../postprocess/ehcsm_sorts.py bySymbol ${DATA_DIR}/ehcsm_bySymbol.txt &
../postprocess/ehcsm_sorts.py byAge ${DATA_DIR}/ehcsm_byAge.txt &
../postprocess/ehcsm_sorts.py byStructure ${DATA_DIR}/ehcsm_byStructure.txt &
../postprocess/ehcsm_sorts.py byExpressed ${DATA_DIR}/ehcsm_byExpressed.txt &
../postprocess/ehcsm_sorts.py byExperiment ${DATA_DIR}/ehcsm_byExperiment.txt &
echo "Started six sorters..."
wait
echo "Sorters finished.  Combining results..."

paste ${DATA_DIR}/ehcsm_key.txt ${DATA_DIR}/ehcsm_bySymbol.txt ${DATA_DIR}/ehcsm_byAge.txt ${DATA_DIR}/ehcsm_byStructure.txt ${DATA_DIR}/ehcsm_byExpressed.txt ${DATA_DIR}/ehcsm_byExperiment.txt > ${DATA_DIR}/ehcsm.txt
echo "Finished.  Removing temp files..."

for suffix in key bySymbol byAge byStructure byExpressed byExperiment
do
rm ${DATA_DIR}/ehcsm_${suffix}.txt
done
echo "Finished.  Deleting table contents..."

psql -h ${PG_FE_DBSERVER} -U ${PG_FE_DBUSER} -d ${PG_FE_DBNAME} <<EOSQL

DELETE FROM expression_ht_consolidated_sample_measurement_sequence_num
;

EOSQL

echo "Finished.  Loading new data..."
PG_FE_DBPASSWORD=`cat ${PG_FE_DBPASSWORDFILE} | grep ${PG_FE_DBUSER} | sed 's/:/ /g' | awk '{print $5}'`
../control/bulkLoadPostgres.sh ${PG_FE_DBSERVER} ${PG_FE_DBNAME} ${PG_FE_DBUSER} ${PG_FE_DBPASSWORD} ${DATA_DIR}/ehcsm.txt expression_ht_consolidated_sample_measurement_sequence_num

echo "Finished at " `uptime | sed 's/ //' | awk '{print $1}'`
