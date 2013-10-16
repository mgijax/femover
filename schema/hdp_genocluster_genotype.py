#!/usr/local/bin/python

import Table

# contains data definition information for the genotype-cluster genotype table.

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
# human disease portal
tableName = 'hdp_genocluster_genotype'

#
# statement to create this table
#
# hdp (human disease portal)
#
# This table represents a genotype that belongs to a specific geno-cluster
#
# see the gatherer for more information
#
createStatement = '''CREATE TABLE %s  ( 
	unique_key              int     not null,
	hdp_genocluster_key	int	not null,
	genotype_key		int	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'hdp_genocluster_key' : 'create index %s on %s (hdp_genocluster_key)',
        'genotype_key' : 'create index %s on %s (genotype_key)',
        }

keys = {
        'genotype_key' : ('genotype', 'genotype_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the grid cluster flower containing data about markers that belong to a given grid cluster',
	Table.COLUMN : {
		'unique_key' : 'unique key identifying this human disease portal geno-cluster',
		'hdp_genocluster_key' : 'genotype-cluster key',
		'genotype_key' : 'genotype that is annotated to the geno-cluster',
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
