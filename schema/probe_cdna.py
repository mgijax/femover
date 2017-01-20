#!/usr/local/bin/python

import Table

# contains data definition information for the probe_cdna table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'probe_cdna'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	probe_key	int		not null,
	age			text	null,
	tissue		text	null,
	cell_line	text	null,
	sequence_num	int	not null,
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
	Table.TABLE : 'petal of probe table; contains a few fields that are specific to cDNA probes; only includes probes with E or P relationships to markers, with mouse as an organism, and with segment type = cDNA.  This includes about 44% of probes.',
	Table.COLUMN : {
		'probe_key' : 'foreign key to probe table; identifies the probe for this record',
		'age' : 'age from the PRB_Source table',
		'tissue' : 'tissue from the PRB_Tissue table',
		'cell_line' : 'cell line from the VOC_Term table',
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
