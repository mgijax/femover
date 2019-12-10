#!/bin/sh

cd `dirname $0`; . ./Configuration

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
    echo "    -C : Print out the config used without running"
    echo "    -d : Disease tables (disease detail page)"
    echo "    -g : Genotypes"
    echo "    -h : IMSR counts (via HTTP)"
    echo "    -i : Images"
    echo "    -m : Markers"
    echo "    -M : Mapping data"
    echo "    -n : Annotations"
    echo "    -o : Disease Portal"
    echo "    -p : Probes"
    echo "    -s : Sequences"
    echo "    -S : Strains"
    echo "    -r : References"
    echo "    -u : Universal expression data (classical + RNA-Seq)"
    echo "    -v : Vocabularies"
    echo "    -x : eXpression (GXD Data plus GXD Literature Index)"
    echo "    -X : high-throughput expression data (from ArrayExpress)"
    echo "    -G : run a single, specified gatherer (name specified afterward)"
    echo "    -Z : skip post-processing tasks (postprocess directory)"
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
POSSIBLE_FLAGS="-a -A -b -c -C -d -g -h -i -m -M -n -o -p -s -t -r -v -x -X -G -S -u -Z"
skipPostProcess=0
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

        if [ "$1" = "-Z" ]; then
            shift
    	    skipPostProcess=1
        fi

        # if setting flags, then ignore single priority tables
        # that is, we don't want the single  priority tables to execute
        # if user has provided flags
        SINGLE_PRIORITY_TABLES=""
        export SINGLE_PRIORITY_TABLES
    fi
    shift
done

# remove any data files from the previous run
if [ "${REMOVE_DATA_FILES}" == "" ]; then
    echo "Missing REMOVE_DATA_FILES setting"
elif [ "${REMOVE_DATA_FILES}" == "1" ]; then
    rm -f ${DATA_DIR}/*
fi

# run the mover with any specified flags (except -Z, which is only for this script)
FLAGS=`echo ${FLAGS} | sed 's/-Z//'`
echo "buildDatabase.py ${FLAGS}"
./buildDatabase.py ${FLAGS}
if [ $? -ne 0 ]; then
	# A non-zero exit code doesn't necessarily mean the run failed, as random bus errors
	# often seem to happen just at the end of the script.  If the script got as far as
	# writing the "Build succeeded" message, then we can just ignore the exit code.
	
	LOG_OKAY=`tail -2 ${LOG_DIR}/buildDatabase.log | grep -c "Build succeeded"`
	if [ ${LOG_OKAY} -eq 0 ]; then
    	echo "Build failed.  See logs for details."
    	exit 1
    fi
fi

# rename log file so subsequent calls will not overwrite
rm -rf ${LOG_DIR}/buildDatabase.log.1
mv -f ${LOG_DIR}/buildDatabase.log ${LOG_DIR}/buildDatabase.log.1

# run the mover for single priority tables
for table in ${SINGLE_PRIORITY_TABLES}
do
    echo "buildDatabase.py -G ${table}"
    ./buildDatabase.py -G ${table}
    if [ $? -ne 0 ]; then
        echo "Single-Priority Build failed.  See logs for details."
        exit 1
    fi

    # rename log file so subsequent calls will not overwrite
    rm -rf ${LOG_DIR}/buildDatabase.${table}.log
    mv -f ${LOG_DIR}/buildDatabase.log ${LOG_DIR}/buildDatabase.${table}.log
done

# run any post-processing scripts
rm -rf ${LOG_DIR}/postprocess.log
if [ ${skipPostProcess} -eq 0 ]; then
	echo "running post processing scripts"
	for postscript in ${FEMOVER}/postprocess/*.sh
	do
    		echo "${postscript}" >> ${LOG_DIR}/postprocess.log
    		${postscript} >> ${LOG_DIR}/postprocess.log
    		if [ $? -ne 0 ]; then
        		echo "${postscript} failed.  See logs for details." >> ${LOG_DIR}/postprocess.log
        		exit 1
    		fi
	done
else
	echo "skipping post processing scripts"
fi

./dbExecute.py 'grant select on all tables in schema fe to public'
