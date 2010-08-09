#!/usr/local/bin/python

import Table

# contains data definition information for the template table
# (search for template to find areas that need changes)

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'template'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	template
	PRIMARY KEY(template))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'template' : 'create index %s on %s (template)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)