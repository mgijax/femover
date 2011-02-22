#!/usr/local/bin/python

import Table

# contains data definition information for the expression_imagepane_details
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_imagepane_details'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		not null,
	imagepane_key	int		not null,
	assay_key	int		not null,
	assay_id	varchar(40)	null,
	marker_key	int		not null,
	marker_id	varchar(40)	null,
	marker_symbol	varchar(50)	null,
	sequence_num	int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'imagepane_key' : 'create index %s on %s (imagepane_key)',
	'assay_key' : 'create index %s on %s (assay_key)',
	'marker_key' : 'create index %s on %s (marker_key)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
