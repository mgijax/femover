#!./python

import Table

# contains data definition information for the disease_row_to_model table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'disease_row_to_model'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int     not null,
        disease_row_key         int     not null,
        disease_model_key       int     not null,
        sequence_num            int     not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'disease_model_key' : 'create index %s on %s (disease_model_key)',
        }

# column name -> (related table, column in related table)
keys = { 'disease_row_key' : ('disease_row', 'disease_row_key'),
        'disease_model_key' : ('disease_model', 'disease_model_key') }

# index used to cluster data in the table
clusteredIndex = ('disease_row_key',
        'create index %s on %s (disease_row_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'This is a join table between a disease_row (possibly representing multiple mouse markers in a homology class) and the mouse models for those markers for the disease',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this row, needed for Hibernate object identification',
                'disease_row_key' : 'foreign key to disease_row, identifiying which disease row has this model',
                'disease_model_key' : 'foreign key to disease_model, identifying a mouse model for the disease row',
                'sequence_num' : 'pre-computed sequence number, to be used to order the mouse models for a given disease row',
                },
        Table.INDEX : {
                'disease_row_key' : 'clustered index, quick access to models for a given disease row',
                'disease_model_key' : 'access disease rows for which a given mouse model exhibits the disease',
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
