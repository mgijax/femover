#!/usr/local/bin/python

import Table

# contains data definition information for the image table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'image'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	image_key		int		not null,
	reference_key		int		not null,
	thumbnail_image_key	int		null,
	fullsize_image_key	int		null,
	is_thumbnail		int		not null,
	width			int		null,
	height			int		null,
	figure_label		varchar(255)	not null,
	image_type		varchar(255)	null,
	pixeldb_numeric_id	varchar(30)	null,
	mgi_id			varchar(30)	null,
	copyright		varchar(512)	null,
	caption			varchar(4500)	null,
	PRIMARY KEY(image_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'reference_key' : 'create index %s on %s (reference_key)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
