import Table

# contains data definition information for the markerAnnotation table

###--- Globals ---###

# name of this database table
tableName = 'markerAnnotation'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	uniqueKey		int(11)		NOT NULL,
	markerKey		int(11)		NOT NULL,
	annotationType		varchar(255)	NOT NULL,
	vocabName		varchar(255)	NOT NULL,
	term			varchar(255)	NOT NULL,
	termID			varchar(30)	NULL,
	annotationKey		int(11)		NULL,
	qualifier		varchar(50)	NULL,
	evidenceTerm		varchar(255)	NULL,
	referenceKey		int(11)		NULL,
	jnumID			varchar(30)	NULL,
	PRIMARY KEY(uniqueKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'markerKey' : 'create index %s on %s (markerKey, annotationType)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
