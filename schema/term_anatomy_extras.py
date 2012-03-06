#!/usr/local/bin/python

import Table

# contains data definition information for the term_anatomy_extras table
# This table contains additional information for anatomical dictionary terms.

###--- Globals ---###

# name of this database table
tableName = 'term_anatomy_extras'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	term_key		int		not null,
	system			varchar(255)	null,
	theiler_stage		varchar(5)	null,
	mgd_structure_key	int		not null,
	edinburgh_key		int		null,
	PRIMARY KEY(term_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'mgd_structure_key' : 'create index %s on %s (mgd_structure_key)',
	'edinburgh_key' : 'create index %s on %s (edinburgh_key)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
