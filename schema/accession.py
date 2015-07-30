#!/usr/local/bin/python

import Table

# contains data definition information for the accession table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'accession'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	object_key		int		not null,
	search_id		text	not null,
	display_id		text	not null,
	sequence_num		int		not null,
	description		text	null,
	logical_db_key		smallint	not null,
	display_type_key	smallint	not null,
	object_type_key		smallint	not null,
	PRIMARY KEY(unique_key))''' % tableName

clusteredIndex = ('lower_acc_id', 'create index %s on %s (lower(search_id))')

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'acc_id' : 'create index %s on %s (search_id)',
	}

keys = {
	'logical_db_key' : ('accession_logical_db', 'logical_db_key'),
	'display_type_key' : ('accession_display_type', 'display_type_key'),
	'object_type_key' : ('accession_object_type', 'object_type_key'),
	}

comments = {
	Table.TABLE : \
	    'main table for accession flower; for searching by accession IDs.  While we often denormalize vocabularies in this database, we opt to normalize three vocabularies into petal tables for this flower, because the accession table is so large.  The space savings makes the normalization worth the added complexity.',
	Table.COLUMN : {
	    'unique_key' : 'unique key for each row; needed to help Hibernate identify unique objects',
	    'object_key' : 'key which identifies the object with which this ID is associated; key is unique within a given data type',
	    'search_id' : 'the ID for which to search',
	    'display_id' : 'the ID to display for a match (so we can search by one ID and display a different one',
	    'sequence_num' : 'used for ordering when multiple rows match the same ID',
	    'description' : 'description to show for the ID and its object',
	    'logical_db_key' : 'foreign key to accession_logical_db, identifies the source (the logical database) of the ID',
	    'display_type_key' : 'foreign key to accession_display_type, identifies the displayable name of the object type which which the ID is associated',
	    'object_type_key' : 'foreign key to accession_object_type, identifies the object type (MGI Type) with which the ID is associated.',
		},
	Table.INDEX : {
	    'acc_id' : 'allows searching by case-sensitive accession ID',
	    'lower_acc_id' : 'allows searching by lowercased accession ID, used to cluster data in the table',
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
