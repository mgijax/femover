#!/usr/local/bin/python

import Table

# contains data definition information for the grid-cluster-marker table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
# human disease portal
tableName = 'hdp_gridcluster_marker'

#
# statement to create this table
#
# hdp (human disease portal)
#
# This table represents the "grid cluster" -> marker relationships
# of the homologene clusters for hdp-specific
# super-simple genotypes (or null genotypes).
#
# plus it adds the mouse/human markers that do NOT exist in the homologene cluster
#
# see the gatherer for more information
#
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int	not null,
	hdp_gridcluster_key	int	not null,
	marker_key		int	not null,
	organism_key		int	not null,
	symbol			text	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'hdp_gridcluster_key' : 'create index %s on %s (hdp_gridcluster_key)',
        'marker_key' : 'create index %s on %s (marker_key)',
        'organism_key' : 'create index %s on %s (organism_key)',
        }

keys = {
        'hdp_gridcluster_key' : ('hdp_gridcluster', 'hdp_gridcluster_key'),
        'marker_key' : ('marker', 'marker_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the grid cluster flower containing data about markers that belong to a given grid cluster',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record, no other purpose',
		'hdp_gridcluster_key' : 'unique key identifying this human disease portal grid cluster',
		'marker_key' : 'marker that is annotated to the grid cluster',
		'organism_key' : 'organism of marker',
		'symbol' : 'symbol of marker',
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
