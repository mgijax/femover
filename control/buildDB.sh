#!/bin/sh

cd `dirname $0`; . ../Configuration

usage()
{
    echo "Usage: buildDB.sh [postgres | mysql] [flags]"
    echo "  Optional target database type:"
    echo "    postgres : build into a PostgreSQL database"
    echo "    mysql : build into a MySQL database"
    echo "  If no target database type is specified, we use the one specified"
    echo "  in the Configuration file."
    echo ""
    echo "  Optional flags for data sets to regenerate:"
    echo "    -a : Alleles"
    echo "    -A : Accession IDs"
    echo "    -b : Batch Query Tables"
    echo "    -c : Cre (Recombinases)"
    echo "    -g : Genotypes"
    echo "    -h : IMSR counts (via HTTP)"
    echo "    -i : Images"
    echo "    -m : Markers"
    echo "    -n : Annotations"
    echo "    -p : Probes"
    echo "    -s : Sequences"
    echo "    -r : References"
    echo "    -v : Vocabularies"
    echo "    -x : eXpression (GXD Data plus GXD Literature Index)"
    echo "    -G : run a single, specified gatherer (name specified afterward)"
    echo "  If no data set flags are specified, the whole front-end database"
    echo "  will be (re)generated.  Any existing contents of the database"
    echo "  will be wiped."

    if [ "$1" != "" ]; then
	    echo "Error: $1"
    fi

    exit 1
}

# handle the first parameter -- which database type to build into

if [ "$1" = "postgres" ]; then
	TARGET_TYPE=postgres
	shift
elif [ "$1" = "mysql" ]; then
	TARGET_TYPE=mysql
	shift
fi
export TARGET_TYPE

# handle the other (optional) parameters

FLAGS=""
POSSIBLE_FLAGS="-a -A -b -c -g -h -i -m -n -p -s -r -v -x -G"
while [ $# -gt 0 ]; do
	found=0
	for flag in ${POSSIBLE_FLAGS}
	do
		if [ "${flag}" = "$1" ]; then
			FLAGS="${FLAGS} ${flag}"
			found=1
		fi
	done

	if [ ${found} -eq 0 ]; then
		usage "Invalid command-line flag: $1"
	else
		# if naming a specific gatherer to run, then pick up
		# the gatherer's name, as well
		if [ "$1" = "-G" ]; then
			shift
			FLAGS="${FLAGS} $1"
		fi
	fi
	shift
done

# run the mover with any specified flags
${FEMOVER}/control/buildDatabase.py ${FLAGS}

if [ $? -ne 0 ]; then
	echo "Build failed.  See logs for details."
	exit 1
fi