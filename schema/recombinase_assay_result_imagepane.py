#!/usr/local/bin/python

import Table

# contains data definition information for the
# recombinase_assay_result_imagepane table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'recombinase_assay_result_imagepane'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	result_key		int		not null,
	image_key		int		not null,
	sequence_num		int		not null,
	pane_label		varchar(255)	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'result_key' : 'create index %s on %s (result_key)',
	}

keys = {
	'result_key' : ('recombinase_assay_result', 'result_key'),
	'image_key' : ('image', 'image_key'),
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
