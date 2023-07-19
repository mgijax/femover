#!./python

import Table

# contains data definition information for the specimen_result_imagepane table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'specimen_result_to_imagepane'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        specimen_result_imagepane_key           int             not null,
        specimen_result_key             int             not null,
        imagepane_key           int             not null,
        imagepane_seq           int             not null,
        PRIMARY KEY(specimen_result_imagepane_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'specimen_result_key' : 'create index %s on %s (specimen_result_key)',
        'imagepane_key' : 'create index %s on %s (imagepane_key)',
        }

keys = {
        'specimen_result_key' : ('specimen_result', 'specimen_result_key'),
        'imagepane_key' : ('expression_imagepane', 'imagepane_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains relationships from specimen_result to expression_imagepane ',
        Table.COLUMN : {
                'specimen_result_imagepane_key' : 'unique identifier for this table',
                'specimen_result_key' : 'unique identifier for this specimen_result',
                'imagepane_key' : 'unique identifier for this expression_imagepane',
                'imagepane_seq' : 'sequence to display imagepanes (if needed)',
                },
        Table.INDEX : {
                'specimen_result_key' : 'foreign key to specimen_result',
                'imagepane_key' : 'foreign key to expression_imagepane',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
                clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
