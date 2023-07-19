#!./python

import Table

# contains data definition information for the uni_by_assaytype table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'uni_by_assaytype'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        uni_key                 int     not null,
        by_assaytype    int     not null,
        PRIMARY KEY(uni_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
        'uni_key' : ('universal_expression_result', 'uni_key'),
}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'uni_by_assaytype',
        Table.COLUMN : {
                'uni_key' : 'foreign key to universal_expression_result, uniquely identifying the expression result that this sequence number is for',
                'by_assaytype' : 'precomputed sequence number for sorting this expression result by assaytype',
                },
        Table.INDEX : {},
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
                clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
