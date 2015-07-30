#!/usr/local/bin/python

import Table

# contains data definition information for the homology_cluster_organism table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'homology_cluster_organism'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	cluster_organism_key	int		not null,
	cluster_key		int		not null,
	organism		text	not null,
	sequence_num		int		not null,
	PRIMARY KEY(cluster_organism_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'cluster_key' : 'create index %s on %s (cluster_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'cluster_key' : ('homology_cluster', 'cluster_key')
	}

# index used to cluster data in the table
clusteredIndex = ('clustered_index',
	'create index %s on %s (cluster_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the homology cluster flower, containing an ordered list of organisms in each cluster',
	Table.COLUMN : {
		'cluster_organism_key' : 'unique key for this cluster/organism pair',
		'cluster_key' : 'identifies the cluster we are referencing',
		'organism' : 'identifies the type of organism',
		'sequence_num' : 'used to order the organisms for a cluster',
		},
	Table.INDEX : {
		'cluster_key' : 'quick retrieval of organisms for a cluster',
		'clustered_index' : 'ensures all organisms for a cluster are grouped together in order',
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
