#!/usr/local/bin/python

import Table

# contains data definition information for the expression_imagepane_set table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_imagepane_set'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	paneset_key	int		not null,
	image_key	int		not null,
	thumbnail_key	int		null,
	assay_type	varchar(80)	null,
	pane_labels	varchar(768)	null,
	marker_key	int		not null,
	in_pixeldb	int		not null,
	sequence_num	int		not null,
	PRIMARY KEY(paneset_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'image_key' : 'create index %s on %s (image_key)',
	'marker_key' : 'create index %s on %s (marker_key)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
