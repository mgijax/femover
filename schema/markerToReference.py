import Table

# contains data definition information for the markerToReference table

###--- Globals ---###

# name of this database table
tableName = 'markerToReference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	uniqueKey	int(11)	NOT NULL,
	markerKey	int(11)	NOT NULL,
	referenceKey	int(11)	NOT NULL,
	qualifier	varchar(80)	NULL,
	PRIMARY KEY(uniqueKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'markerKey' : 'create index %s on %s (markerKey, referenceKey)',
	'referenceKey' : 'create index %s on %s (referenceKey, markerKey)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
