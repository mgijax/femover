import Table

# contains data definition information for the referenceID table

###--- Globals ---###

# name of this database table
tableName = 'referenceID'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	uniqueKey	int(11) NOT NULL,
	referenceKey 	int(11) NOT NULL,
	logicalDB    	varchar(80) NULL,
	accID	  	varchar(30) NULL,
	preferred	int(1) NOT NULL,
	private		int(1) NOT NULL,
	PRIMARY KEY(uniqueKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'referenceKey' : 'create index %s on %s (referenceKey)',
	'accID' : 'create index %s on %s (accID)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
