#!/usr/local/bin/python

import Table

# contains data definition information for the marker_link table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_link'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		not null,
	marker_key	int		not null,
	link_group	varchar(30)	not null,
	sequence_num	int		not null,
	associated_id	varchar(30)	null,
	display_text	varchar(255)	not null,
	url		varchar(255)	not null,
	has_markups	int		not null,
	use_new_window	int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = { 'marker_key' : ('marker', 'marker_key') }

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'stores pre-packaged links for a marker to be displayed on fewi pages',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this particular record',
		'marker_key' : 'foreign key to the marker table, identifying which marker this link is for',
		'link_group' : 'identifies which group of links this link is part of',
		'sequence_num' : 'orders the links for a given marker/link group pair',
		'associated_id' : 'the accession ID associated with this link, if any',
		'display_text' : 'of text which should be displayed as the clickable text of the link',
		'url' : 'URL that the link should go to (not parameterized for IDs -- any IDs, coordinates, etc. should already be plugged in)',
		'has_markups' : 'are there any MGI-specific markups in the url field such that fewi should run the URL through the NotesTagConverter?  (0/1)  This allows us to point MGI links to various developer, production, or public instances, etc.',
		'use_new_window' : 'should this link open a new window/tab when clicked? (0/1)',
		},
	Table.INDEX : {
		'marker_key' : 'quick lookup of all pre-compuated links for a marker',
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
