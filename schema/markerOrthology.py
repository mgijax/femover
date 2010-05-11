import Table

# contains data definition information for the markerOrthology table

###--- Globals ---###

# name of this database table
tableName = 'markerOrthology'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	uniqueKey	int(11)	NOT NULL,
	mouseMarkerKey	int(11) NOT NULL,
	otherMarkerKey	int(11)	NOT NULL,
	otherSymbol	varchar(50)	NULL,
	otherOrganism	varchar(50)	NULL,
	PRIMARY KEY(uniqueKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'mouseMarker' : 'create index %s on %s (mouseMarkerKey)',
	'otherMarker' : 'create index %s on %s (otherMarkerKey)',
	'otherOrganism' : 'create index %s on %s (otherOrganism)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
