#!/usr/local/bin/python

import Table

# contains data definition information for the marker_mrm_property table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_mrm_property'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int	not null,
	mrm_key			int	not null,
	name			text	not null,
	value			text	not null,
	sequence_num		int	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
	'mrm_key' : ('marker_related_marker', 'mrm_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('mrmSeqNum',
	'create index %s on %s (mrm_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'A record in this table represents a (name, value) property for a marker-to-marker relationship',
	Table.COLUMN : {
		'unique_key' : 'primary key, uniquely identifying a record in this table',
		'mrm_key' : 'foreign key, identifying the marker-to-marker relationship for this property',
		'name' : 'name of the property',
		'value' : 'value of the property, as a string',
		'sequence_num' : 'integer, used to order properties for a given marker-to-marker relationship',
		},
	Table.INDEX : {
		'mrmSeqNum' : 'clustered index ensures properties for a given relationship are stored in order together on disk',
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