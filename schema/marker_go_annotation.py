#!/usr/local/bin/python

import Table

# contains data definition information for the marker_go_annotation table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_go_annotation'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	go_annotation_key	int		not null,
	marker_key		int		not null,
	dag_name		varchar(80)	null,
	qualifier		varchar(20)	null,
	term			varchar(255)	null,
	term_id			varchar(40)	null,
	evidence_code		varchar(5)	null,
	reference_count		int		not null,
	inferred_id_count	int		not null,
	sequence_num		int		not null,
	PRIMARY KEY(go_annotation_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'marker_key' : 'create index %s on %s (marker_key)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
