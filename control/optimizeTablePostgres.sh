#!/bin/sh

# run 'vacuum' and 'analyze' on a Postgres table to optimize its size and
# update its data statistics (needed for the query planner to make good
# choices)

# Usage: optimizeTablePostgres.sh <server> <database> <user> <password> <table name>

. ./Configuration

PGPASSWORD=$4
export PGPASSWORD

psql -h $1 -d $2 -U $3 --command "vacuum analyze $5;" > ${LOG_DIR}/$5.optimize.log 2>&1
exit $?
