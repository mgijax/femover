#!/usr/local/bin/python

import Table

# contains data definition information for the annotation_sequence_num table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'annotation_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	annotation_key		int	not null,
	by_dag_structure	int	not null,
	by_term_alpha		int	not null,
	by_vocab		int	not null,
	by_annotation_type	int	not null,
	PRIMARY KEY(annotation_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'annotation_key' : ('annotation', 'annotation_key') }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
