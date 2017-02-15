#!/usr/local/bin/python

import Table

# contains data definition information for the mapping_experiment table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mapping_experiment'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	experiment_key		int		not null,
	experiment_type		text	not null,
	experiment_note		text	null,
	reference_key		int		not null,
	reference_note		text	null,
	chromosome			text	not null,
	modification_date	text	null,
	primary_id			text	null,
	PRIMARY KEY(experiment_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'reference_key' : 'create index %s on %s (reference_key)',
	'primary_id' : 'create index %s on %s (primary_id)',
	'experiment_type' : 'create index %s on %s (experiment_type)',
	}

# column name -> (related table, column in related table)
keys = {
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for mapping flower, containing basic data common across experiment types',
	Table.COLUMN : {
		'experiment_key' : 'unique database key for the mapping experiment; same as MLD_Expts._Expt_key in production',
		'experiment_type' : 'type of mapping experiment',
		'experiment_note' : 'note for mapping experiment',
		'reference_key' : 'foreign key to reference table, identifying publication for this experiment',
		'reference_note' : 'note for this reference regarding this experiment',
		'chromosome' : 'chromosome studied in this experiment',
		'modification_date' : 'date that data for this mapping experiment was last modified',
		'primary_id' : 'primary MGI ID for the experiment',
		},
	Table.INDEX : {
		'reference_key' : 'quickly retrieve experiments by reference',
		'primary_id' : 'supports fast lookup of experiment by ID',
		'experiment_type' : 'lookup by type of experiment, often used in combination with other fields',
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
