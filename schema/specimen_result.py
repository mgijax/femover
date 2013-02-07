#!/usr/local/bin/python

import Table

# contains data definition information for the specimen_result table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'specimen_result'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	specimen_result_key		int		not null,
	specimen_key		int		not null,
	structure		text	not null,
	structure_mgd_key		int		not null,
	level			text	null,
	pattern			text	null,
	note			text	null,
	specimen_result_seq		int		not null,
	PRIMARY KEY(specimen_result_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'specimen_key' : 'create index %s on %s (specimen_key)',
	'structure_mgd_key' : 'create index %s on %s (structure_mgd_key)',
	}

keys = {
	'specimen_key' : ('assay_specimen', 'specimen_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains information about assay_specimen results',
	Table.COLUMN : {
		'specimen_result_key' : 'unique identifier for this specimen_result',
		'specimen_key' : 'unique identifier for this specimen, same as _specimen_key in mgd',
		'structure' : 'the display text for the result\'s structure, includes TS and printname',
		'structure_mgd_key' : '_structure_key from mgd (for linking purposes)',
		'level' : 'detection level',
		'pattern' : 'pattern of detection (blank if not specified)',
		'note' : 'result note (if exists)',
		'specimen_result_seq' : 'display order of results',
		},
	Table.INDEX : {
		'specimen_key' : 'foreign key to assay_specimen',
		'structure_mgd_key' : 'can be joined with certain vocabulary tables',
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
