#!./python

import Table

# Contains incidental mutation filenames for the allele detail page

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_incidental_mut'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        allele_key              int             not null,
        filename                text            not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'allele_key': 'create index %s on %s (allele_key)',
}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'represents the Non-Normal systems displayed on summary page',
        Table.COLUMN : {
                'unique_key' : 'unique key identifying this row',
                'allele_key' : 'key for the allele',
                'filename' : 'filename of the incidental mutation file',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments,
                clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
