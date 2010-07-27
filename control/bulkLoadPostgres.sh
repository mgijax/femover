#!/bin/sh

# bulk load an input file into a Postgres table
# Usage: bulkLoadPostgres.sh <server> <database> <user> <password> <input file> <table name>

PGPASSWORD=$4
export PGPASSWORD

psql -h $1 -d $2 -U $3 <<END
\copy $6 from '$5' with null as ''
\g
vacuum analyze $6;
END
exit $?
