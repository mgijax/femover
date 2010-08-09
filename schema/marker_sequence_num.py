#!/usr/local/bin/python

import Table

# contains data definition information for the marker_sequence_num table

###--- Globals ---###

# name of this database table
tableName = 'marker_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	marker_key	int		not null,
	by_symbol	int		not null,
	by_marker_type	int		not null,
	by_organism	int		not null,
	by_primary_id	int		not null,
	by_location	int		not null,
	PRIMARY KEY(marker_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
