#!/usr/local/bin/python

import Table

# contains data definition information for the actual_database table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'actual_database'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	actualdb_key	int		not null,
	logical_db	varchar(80)	null,
	actual_db	varchar(80)	null,
	url		varchar(255)	null,
	sequence_num	int		not null,
	PRIMARY KEY(actualdb_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}
clusteredIndex = ('logical_db', 'create index %s on %s (logical_db)')
 
# column name -> (related table, column in related table)
keys = {}

comments = {
	Table.TABLE : 'standalone table; contains actual databases and their URLs for each logical database.  Each logical database can have multiple actual databases, and hence multiple rows in this table.',
	Table.COLUMN : {
	    'actualdb_key' : 'unique key identifying the actual database (same as _ActualDB_key in mgd)',
	    'logical_db' : 'name of the logical database for this actual database',
	    'actual_db' : 'name of the actual database for this row',
	    'url' : 'URL to use when linking to an ID for this actual database (contains @@@@ where the ID should be substituted)',
	    'sequence_num' : 'provided for ordering the actual databases of a logical database',
		},
	Table.INDEX : {
	    'logical_db' : 'provided for finding the actual databases for a desired logical database.  Used to cluster the data.',
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
