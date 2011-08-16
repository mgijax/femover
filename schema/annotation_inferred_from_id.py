#!/usr/local/bin/python

import Table

# contains data definition information for the annotation_inferred_from_id
# table

###--- Globals ---###

# name of this database table
tableName = 'annotation_inferred_from_id'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		NOT NULL,
	annotation_key		int		NOT NULL,
	logical_db		varchar(80)	 NULL,
	acc_id			varchar(30)	 NULL,
	preferred		int		NOT NULL,
	private			int		NOT NULL,
	sequence_num		int		NOT NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'annotation_key' : 'create index %s on %s (annotation_key)',
	}

keys = { 'annotation_key' : ('annotation', 'annotation_key') }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
