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

keys = {
	'image_key' : ('image', 'image_key'),
	'thumbnail_key' : ('image', 'image_key'),
	'marker_key' : ('marker', 'marker_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the image flower, containing data for pane sets (where each pane set is a unique image/marker/assay_type combination).  This allows easy display of all pane labels for a given image/marker/assay_type, for instance when showing all expression images for a gene.',
	Table.COLUMN : {
		'paneset_key' : 'unique key for this pane set (this image/marker/assay_type combination)',
		'image_key' : 'identifies the image',
		'thumbnail_key' : 'key for the thumbnail image corresponding to this image',
		'assay_type' : 'type of expression assay',
		'pane_labels' : 'comma-delimited list of pane labels',
		'marker_key' : 'identifies the marker',
		'in_pixeldb' : '1 if this image is in pixeldb (and is thus displayable), 0 if not',
		'sequence_num' : 'used to order the pane sets for a given image',
		},
	Table.INDEX : {
		'image_key' : 'quick access to pane sets for a given image',
		'marker_key' : 'quick access to pane sets for a given marker',
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
