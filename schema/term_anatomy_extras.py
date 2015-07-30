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
	system			text	null,
	theiler_stage		text	null,
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

# index used to cluster data in the table
clusteredIndex = None

keys = {
        'term_key' : ('term', 'term_key'),
	}

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains additional data needed only for anatomical dictionary terms (structures)',
	Table.COLUMN : {
		'term_key' : 'foreign key to term table; at most one record in this table for each record in the term table',
		'system' : 'name of the anatomical system of which this term (structure) is a part',
		'theiler_stage' : 'Theiler Stage which includes this structure',
		'mgd_structure_key' : 'value of _Structure_key for this structure in the mgd database',
		'edinburgh_key' : 'value of the Edinburgh key for this structure, facilitating links from Edinburgh',
		},
	Table.INDEX : {
		'mgd_structure_key' : 'look up structures by mgd _Structure_key',
		'edinburgh_key' : 'look up structures by Edinburgh key',
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
