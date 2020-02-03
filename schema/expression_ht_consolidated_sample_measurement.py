#!/usr/local/bin/python

import Table

# contains data definition information for the expression_ht_consolidated_sample_measurement table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_ht_consolidated_sample_measurement'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	consolidated_measurement_key	integer				not null,
	consolidated_sample_key			integer				not null,
	marker_key						integer				not null,
	level							text				null,
	biological_replicate_count		integer				not null,
	average_qn_tpm					double precision	not null,
	PRIMARY KEY(consolidated_measurement_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'consolidated_sample_key' : 'create index %s on %s (consolidated_sample_key)',
	'marker_key' : 'create index %s on %s (marker_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'marker_key' : ('marker', 'marker_key'),
	'consolidated_sample_key' : ('expression_ht_consolidated_sample', 'consolidated_sample_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'Contains data for RNA-Seq experiments.  Has one record for each (consolidated sample)/marker pair, where a consolidated sample is for a set of biological replicates.',
	Table.COLUMN : {
		'consolidated_measurement_key' : 'primary key for this table; identifies the "combined sample", the set of samples that are biological replicates',
		'marker_key' : 'identifies the marker for this measurement',
		'biological_replicate_count' : 'number of samples in this set of biological replicates (can be a 1 for singleton samples)',
		'average_qn_tpm' : 'quantile normalized Transcripts Per (kilobase) Million, averaged across the set of biological replicates.',
		'level' : 'textual description for the level of expression',
		},
	Table.INDEX : {
		'consolidated_sample_key' : 'quick lookup for all measurements for a consolidated sample by key',
		'marker_key' : 'quick lookup for all measurements for a marker by key',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
