#!./python

import Table

# contains data definition information for the disease_model_to_reference table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'disease_model_to_reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int     not null,
        disease_model_key       int     not null,
        reference_key           int     not null,
        sequence_num            int     not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = { 'reference_key' : 'create index %s on %s (reference_key)' }

# column name -> (related table, column in related table)
keys = { 'disease_model_key' : ('disease_model', 'disease_model_key'),
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('disease_model_key',
        'create index %s on %s (disease_model_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table bewteen disease models and their references',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this row, needed for Hibernate object identification',
                'disease_model_key' : 'foreign key to disease_model, identifying which disease model has this reference',
                'reference_key' : 'foreign key to reference, identifying one reference for this disease model',
                'sequence_num' : 'pre-computed integer to use when sorting references for a disease model',
                },
        Table.INDEX : {
                'reference_key' : 'quick access to all disease models for a reference',
                'disease_model_key' : 'clustered index, ensuring quick access to all references for a disease model',
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
