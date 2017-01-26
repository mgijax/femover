#!/usr/local/bin/python

import Table

# contains data definition information for the probe_sequence_num table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'probe_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	probe_key		int		NOT NULL,
	by_name			int		NOT NULL,
	by_type			int		NOT NULL,
	PRIMARY KEY(probe_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
	'probe_key' : ('probe', 'probe_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains pre-computed sequence numbers for ordering probes',
	Table.COLUMN : {
		'probe_key' : 'identifies the probe',
		'by_name' : 'smart-alpha ordering by probe name, then type, then primary ID',
		'by_type' : 'smart-alpha ordering by probe type, then name, then primary ID',
		},
	Table.INDEX : {},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
