#!/usr/local/bin/python

import Table

# contains data definition information for the grid cluster notation table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
# human disease portal
tableName = 'hdp_genocluster'

#
# statement to create this table
#
# hdp (human disease portal)
#
# This table represents the unique "geno-cluster" key.
#
# see the gatherer for more information
#
createStatement = '''CREATE TABLE %s  ( 
	hdp_genocluster_key	int	not null,
	marker_key		int	not null,
	allele_key_1		int	not null,
	allele_key_2		int	null,
	pairstate_key		int	not null,
	is_conditional		int	not null,
	exists_as_key		int	not null,
	PRIMARY KEY(hdp_genocluster_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'hdp_genocluster_key' : 'create index %s on %s (hdp_genocluster_key)',
        'marker_key' : 'create index %s on %s (marker_key)',
        'allele_key_1' : 'create index %s on %s (allele_key_1)',
        'allele_key_2' : 'create index %s on %s (allele_key_2)',
        'pairstate_key' : 'create index %s on %s (pairstate_key)',
        'exists_as_key' : 'create index %s on %s (exists_as_key)',
	}

keys = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the grid cluster petal, containing one row for each human disease portal grid cluster',
	Table.COLUMN : {
		'hdp_genocluster_key' : 'unique key identifying this human disease portal genotype cluster',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
