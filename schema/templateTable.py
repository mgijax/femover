import Table

# contains data definition information for the Template table
# (search for Template to find areas that need changes)

###--- Globals ---###

# name of this database table
tableName = 'Template'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	Template
	PRIMARY KEY(Template))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'Template' : 'create index %s on %s (Template)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
