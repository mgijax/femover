#!/usr/local/bin/python

import Table

# contains data definition information for the assay_specimen table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'assay_specimen'

# PgSQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	specimen_key		int		not null,
	assay_key		int		not null,
	specimen_label		varchar(255)	null,
	specimen_seq		int		not null,
	PRIMARY KEY(specimen_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'assay_key' : 'create index %s on %s (assay_key)',
	}

keys = {
	'assay_key' : ('expression_assay', 'assay_key'),
	}

clusteredIndex=None
# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'Contains information about specimens for expression_assay objects',
	Table.COLUMN : {
		'specimen_key' : 'unique identifier for this assay, same as _Specimen_key in mgd',
		'assay_key' : 'unique identifier for this assay, same as _Assay_key in mgd',
		'specimen_label' : 'label for the specimen',
		'specimen_seq' : 'sequencenum from mgd',
		},
	Table.INDEX : {
		'assay_key' : 'foreign key to expression_assay',
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
