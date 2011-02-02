#!/usr/local/bin/python

import Table

# contains data definition information for the annotation table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'annotation'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	annotation_key		int		not null,
	dag_name		varchar(80)	null,
	qualifier		varchar(20)	null,
	vocab_name		varchar(40)	null,
	term			varchar(255)	null,
	term_id			varchar(40)	null,
	evidence_code		varchar(5)	null,
	object_type		varchar(80)	null,
	annotation_type		varchar(255)	null,
	reference_count		int		not null,
	inferred_id_count	int		not null,
	PRIMARY KEY(annotation_key))''' % tableName

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
