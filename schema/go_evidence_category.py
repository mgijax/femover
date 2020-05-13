#!./python

import Table

# contains data definition information for the go_evidence_category table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'go_evidence_category'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        evidence_code           text    not null,
        evidence_category       text    not null,
        PRIMARY KEY(evidence_code))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'identifies the general category for each GO evidence code',
        Table.COLUMN : {
                'evidence_code' : 'abbreviation for the evidence term',
                'evidence_category' : 'general grouping containing the evidence code',
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
