import Table

###--- Globals ---###

# name of this database table
tableName = 'marker'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	markerKey    	int(11) NOT NULL,
	symbol        	varchar(50) NULL,
	name          	varchar(255) NULL,
	markerType	varchar(80) NULL,
	markerSubtype   varchar(50) NULL,
	organism      	varchar(50) NULL,
	primaryID      	varchar(30) NULL,
	logicalDB	varchar(80) NULL,
	status       	varchar(255) NULL,
	PRIMARY KEY(markerKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'symbol' : 'create index %s on %s (symbol)',
	'ID' : 'create index %s on %s (primaryID)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
