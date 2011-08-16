#!/usr/local/bin/python

import Table

# contains data definition information for the
# recombinase_assay_result_sequence_num table

###--- Globals ---###

# name of this database table
tableName = 'recombinase_assay_result_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	result_key		int		not null,
	by_structure		int		not null,
	by_age			int		not null,
	by_level		int		not null,
	by_pattern		int		not null,
	by_jnum_id		int		not null,
	by_assay_type		int		not null,
	by_reporter_gene	int		not null,
	by_detection_method	int		not null,
	by_assay_note		int		not null,
	by_allelic_composition	int		not null,
	by_sex			int		not null,
	by_specimen_note	int		not null,
	by_result_note		int		not null,
	PRIMARY KEY(result_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'result_key' : ('recombinase_assay_result', 'result_key'), }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
