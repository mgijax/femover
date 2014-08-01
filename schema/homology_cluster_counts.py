#!/usr/local/bin/python

import Table

# contains data definition information for the homology_cluster_counts table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'homology_cluster_counts'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	cluster_key		int	not null,
	mouse_marker_count	int	not null,
	human_marker_count	int	not null,
	rat_marker_count	int	not null,
	cattle_marker_count	int	not null,
	chimp_marker_count	int	not null,
	dog_marker_count	int	not null,
	monkey_marker_count	int	not null,
	chicken_marker_count	int	not null,
	xenopus_marker_count	int	not null,
	zebrafish_marker_count	int	not null,
	PRIMARY KEY(cluster_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = { 'cluster_key' : ('homology_cluster', 'cluster_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : None,
	Table.COLUMN : {
		'cluster_key' : 'foreign key to homology_cluster, identifying the homology cluster',
		'mouse_marker_count' : 'count of mouse markers in the cluster',
		'human_marker_count' : 'count of human markers in the cluster',
		'rat_marker_count' : 'count of rat markers in the cluster',
		'cattle_marker_count' : 'count of cattle markers in the cluster',
		'chimp_marker_count' : 'count of chimp markers in the cluster',
		'dog_marker_count' : 'count of dog markers in the cluster',
		'monkey_marker_count' : 'count of monkey (rhesus macaque) markers in the cluster',
		'chicken_marker_count' : 'count of chicken markers in the cluster',
		'xenopus_marker_count' : 'count of xenopus markers in the cluster',
		'zebrafish_marker_count' : 'count of zebrafish markers in the cluster',
		},
	Table.INDEX : {},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
