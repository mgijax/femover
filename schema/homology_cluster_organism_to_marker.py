#!/usr/local/bin/python

import Table

# contains data definition information for the
# homology_cluster_organism_to_marker table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'homology_cluster_organism_to_marker'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	cluster_organism_key	int		not null,
	marker_key		int		not null,
	reference_key		int		null,
	qualifier		varchar(80)	null,
	sequence_num		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'co_key' : 'create index %s on %s (cluster_organism_key)',
	'marker_key' : 'create index %s on %s (marker_key)',
	'reference_key' : 'create index %s on %s (reference_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'cluster_organism_key' : ('homology_cluster_organism', 'cluster_organism_key'),
	'marker_key' : ('marker', 'marker_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('clustered_index',
	'create index %s on %s (cluster_organism_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'association table between homology cluster flower and marker flower, identifying the ordered list of markers for each organism in each cluster',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this organism/cluster/marker triple',
		'cluster_organism_key' : 'identifies the cluster/organism pair for this marker',
		'marker_key' : 'identifies the marker',
		'reference_key' : 'optional reference for this marker/organism/cluster triple',
		'qualifier' : 'optional qualifier for this marker/organism/cluster triple',
		'sequence_num' : 'orders the marker for each cluster/organism pair',
		},
	Table.INDEX : {
		'co_key' : 'used to look up markers for a cluster/organism pair',
		'marker_key' : 'used to look up cluster/organism pairs that a given marker is part of',
		'reference_key' : 'used to look up all cluster/organism/marker triples from a given reference',
		'clustered_index' : 'used to cluster the data so markers for each cluster/organism pair are stored together in order',
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
