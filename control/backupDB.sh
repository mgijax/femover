#!/bin/sh

# Purpose: to make a backup of a front-end database

cd `dirname $0`; . ../Configuration

usage()
{
    echo "Usage: backupDB.sh [postgres | mysql]"
    echo "  Optional target database type:"
    echo "    postgres : backup a PostgreSQL database"
    echo "    mysql : backup a MySQL database"
    echo "  If no target database type is specified, we use the one specified"
    echo "  in the Configuration file."

    if [ "$1" != "" ]; then
	    echo "Error: $1"
    fi

    exit 1
}

# handle the first parameter -- which database type to build into

if [ "$1" = "postgres" ]; then
	TARGET_TYPE=postgres
elif [ "$1" = "mysql" ]; then
	TARGET_TYPE=mysql
fi
export TARGET_TYPE

# handle the other (optional) parameters

HOST=`./printDbParameters.py -h`
DATABASE=`./printDbParameters.py -d`
USER=`./printDbParameters.py -u`
PASSWORD=`./printDbParameters.py -p`

# run the right backup program for Postgres / MySQL
if [ "${TARGET_TYPE}" = "postgres" ]; then
    echo "Making backup of ${HOST}..${DATABASE} to ${FE_POSTGRES_DUMP}..."
    PGPASSWORD=${PASSWORD}
    export PGPASSWORD
    ${POSTGRES_HOME}/bin/pg_dump -f ${FE_POSTGRES_DUMP} -Fc -h ${HOST} -U ${USER} ${DATABASE}
    EXITCODE=$?
else
    echo "Making backup of ${HOST}..${DATABASE} to ${FE_MYSQL_DUMP}..."
    ${MYSQL_HOME}/bin/mysqldump --user=${USER} --password=${PASSWORD} --host=${HOST} --skip-lock-table -r ${FE_MYSQL_DUMP} ${DATABASE}
    EXITCODE=$?

    if [ ${EXITCODE} -ne 0 ]; then
	    echo "Compressing ${FE_MYSQL_DUMP}..."
	    /usr/local/bin/gzip -f ${FE_MYSQL_DUMP}
    fi
fi

if [ ${EXITCODE} -ne 0 ]; then
	echo "Backup failed."
	exit 1
fi
