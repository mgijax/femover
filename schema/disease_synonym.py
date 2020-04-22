#!./python

import Table

# contains data definition information for the disease_synonym table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'disease_synonym'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             not null,
        disease_key     int             not null,
        synonym         text    not null,
        sequence_num    int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'synonym' : 'create index %s on %s (synonym)',
        }

# column name -> (related table, column in related table)
keys = { 'disease_key' : ('disease', 'disease_key') }

# index used to cluster data in the table
clusteredIndex = ('disease_key', 'create index %s on %s (disease_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains synonyms for diseases',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this synonym/disease pair',
                'disease_key' : 'foreign key to disease table',
                'synonym' : 'the synonym itself',
                'sequence_num' : 'used to order synonyms for each disease',
                },
        Table.INDEX : {
                'synonym' : 'case-sensitive searching by synonym',
                'disease_key' : 'clustered index to keep synonyms for each disease together',
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
