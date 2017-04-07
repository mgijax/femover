#!/usr/local/bin/python

import Table

# contains data definition information for the mapping_to_marker table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mapping_to_marker'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	experiment_key	int		not null,
	marker_key		int		not null,
	allele_key		int		null,
	assay_type		text	not null,
	description		text	null,
	sequence_num	int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'marker_key' : 'create index %s on %s (marker_key)',
	'allele_key' : 'create index %s on %s (allele_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'experiment_key' : ('mapping_experiment', 'experiment_key'),
	'marker_key' : ('marker', 'marker_key'),
	'allele_key' : ('allele', 'allele_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('experiment_key', 'create index %s on %s (experiment_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between mapping experiments and markers, also includes alleles where data are available',
	Table.COLUMN : {
		'unique_key' : 'generated primary key for this table, no other significance',
		'experiment_key' : 'foreign key to experiment table, identifies the experiment',
		'marker_key' : 'foreign key to marker table, identifies the marker object',
		'allele_key' : 'foreign key to allele table, identifies the allele object (where available)',
		'assay_type' : 'type of assay for the marker',
		'description' : 'extra description for the experiment/marker pair',
		'sequence_num' : 'curator-assigned sequence number to order markers for an experiment',
		},
	Table.INDEX : {
		'marker_key' : 'fast retrieval of experiments for a marker',
		'allele_key' : 'fast retrieval of experiments for an allele',
		'experiment_key' : 'clustered index for grouping all markers of an experiment together for fast retrieval',
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
