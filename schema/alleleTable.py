import Table

# contains data definition information for the allele table

###--- Globals ---###

# name of this database table
tableName = 'allele'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	alleleKey		int(11)		not null,
	symbol			varchar(60)	null,
	name			varchar(255)	null,
	onlyAlleleSymbol	varchar(60)	null,
	primaryID		varchar(30)	null,
	logicalDB		varchar(80)	null,
	alleleType		varchar(255)	null,
	alleleSubType		varchar(255)	null,
	PRIMARY KEY(alleleKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'symbol' : 'create index %s on %s (symbol)',
	'primaryID' : 'create index %s on %s (primaryID)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
