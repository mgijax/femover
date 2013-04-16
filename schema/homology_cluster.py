#!/usr/local/bin/python

import Table

# contains data definition information for the homology_cluster table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'homology_cluster'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	cluster_key	int		not null,
	primary_id	varchar(30)	not null,
	version		varchar(255)	null,
	cluster_date	varchar(12)	null,
	source		varchar(255)	not null,
	PRIMARY KEY(cluster_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'primary_id' : 'create index %s on %s (primary_id)',
	'source' : 'create index %s on %s (source)',
	}

# column name -> (related table, column in related table)
keys = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table defining the homology clusters, containing basic info about each',
	Table.COLUMN : {
		'cluster_key' : 'unique key identifying this homology cluster',
		'primary_id' : 'primary accession ID for this cluster',
		'version' : 'data version number for this cluster',
		'cluster_date' : 'date for this cluster',
		'source' : 'source for this cluster - what group compiled the data',
		},
	Table.INDEX : {
		'primary_id' : 'look up clusters by ID',
		'source' : 'look up all clusters for a given source (eg- Homologene)',
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
