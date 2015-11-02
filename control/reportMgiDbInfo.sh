#!/bin/sh

cd `dirname $0`; . ./Configuration

usage()
{
    echo "Usage: reportMgiDbInfo.sh {postgres | mysql}"
    echo "  Report data from the MGI_dbInfo table in the given source database"

    if [ "$1" != "" ]; then
	    echo "Error: $1"
    fi

    exit 1
}

# check that we have at least one parameter

if [ $# -lt 1 ]; then
	usage "Must specify a source database type"
fi

# handle the first parameter -- database type from which to read

if [ "$1" = "postgres" ]; then
	SOURCE_TYPE=postgres
elif [ "$1" = "mysql" ]; then
	SOURCE_TYPE=mysql
else
	usage "Invalid database type"
fi
export SOURCE_TYPE

# run the Python script to do the reporting
reportMgiDbInfo.py

if [ $? -ne 0 ]; then
	echo "Failed to read from MGI_dbInfo in $1 database"
	exit 1
fi
