#!/bin/bash

# bulk load an input file into a Postgres table
# Usage: bulkLoadPostgres.sh <server> <database> <user> <password> <input file> <table name>

. ./Configuration
echo $PATH

# commented out, as this was causing process to hang when run in background
# from command-line
#which psql

PGPASSWORD=$4
export PGPASSWORD

psql -h $1 -d $2 -U $3 --command "\copy $6 from '$5' with null as ''" > ${LOG_DIR}/$6.load.log 2>&1
EXITCODE=$?

if [ ${EXITCODE} -ne 0 ]; then
	exit ${EXITCODE}
fi

psql -h $1 -d $2 -U $3 --command "vacuum analyze $6;" > ${LOG_DIR}/$6.vacuum.log 2>&1
exit $?
