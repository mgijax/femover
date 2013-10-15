#!/usr/local/bin/python

import Table

# contains data definition information for the queryform_option table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'queryform_option'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	option_key	int	not null,
	form_name	text	not null,
	field_name	text	not null,
	display_value	text	not null,
	submit_value	text	not null,
	help_text	text	null,
	sequence_num	int	not null,
	indent_level	int	null,
	object_count	int	null,
	object_type	text	null,
	show_expanded	int	not null,
	PRIMARY KEY(option_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'form_name' : 'create index %s on %s (form_name)',
	'field_name' : 'create index %s on %s (field_name)',
	}

# column name -> (related table, column in related table)
keys = {}

# index used to cluster data in the table
clusteredIndex = ('to_cluster',
	'create index %s on %s (form_name, field_name, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'queryform_option',
	Table.COLUMN : {
		'option_key' : 'uniquely identifies this option (needed for Hibernate)',
		'form_name' : 'name of the query form containing this option',
		'field_name' : 'name of the particular field on the query form',
		'display_value' : 'text of this option, as it should display',
		'submit_value' : 'value to be submitted when this option is selected',
		'sequence_num' : 'orders options for a particular field',
		'indent_level' : '(optional) indentation level among options for a particular field',
		'object_count' : '(optional) count of objects related to this option',
		'object_type' : '(optional) type of objects that were counted',
		},
	Table.INDEX : {
		'form_name' : 'index by form name',
		'field_name' : 'index by field name',
		'to_cluster' : 'cluster by form name, field name, and sequence num',
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
