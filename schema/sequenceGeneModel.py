import Table

# contains data definition information for the sequenceGeneModel table

###--- Globals ---###

# name of this database table
tableName = 'sequenceGeneModel'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	sequenceKey		int(11)		NOT NULL,
	markerType		varchar(80)	NOT NULL,
	biotype			varchar(255)	NOT NULL,
	exonCount		int(11)		NOT NULL,
	transcriptCount		int(11)		NULL,
	PRIMARY KEY(sequenceKey))''' % tableName

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
