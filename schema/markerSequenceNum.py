import Table

# contains data definition information for the markerSequenceNum table

###--- Globals ---###

# name of this database table
tableName = 'markerSequenceNum'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	markerKey	int(11)		not null,
	bySymbol	int(11)		not null,
	byMarkerType	int(11)		not null,
	byOrganism	int(11)		not null,
	byPrimaryID	int(11)		not null,
	byLocation	int(11)		not null,
	PRIMARY KEY(markerKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
