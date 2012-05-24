#!/usr/local/bin/python

import Table

# contains data definition information for the marker_to_expression_image table

###--- Globals ---###

# name of this database table
tableName = 'marker_to_expression_image'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		not null,
	marker_key	int		not null,
	image_key	int		not null,
	reference_key	int		null,
	qualifier	varchar(80)	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'image_key' : 'create index %s on %s (image_key)',
	'reference_key' : 'create index %s on %s (reference_key)',
	}

keys = {
	'marker_key' : ('marker', 'marker_key'),
	'image_key' : ('image', 'image_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between the marker and image flowers for expression images',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this association',
		'marker_key' : 'identifies the marker',
		'image_key' : 'identifies the image',
		'reference_key' : 'reference supporting the relationship',
		'qualifier' : 'qualifier describing the relationship',
		},
	Table.INDEX : {
		'marker_key' : 'clusters the data so that expression images for a marker are stored together on disk for quick access',
		'image_key' : 'look up markers for an image',
		'reference_key' : 'look up all marker/image pairs for a given reference',
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
