#!./python

import Table

# contains data definition information for the image_sequence_num table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'image_sequence_num'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        image_key               int     not null,
        by_default              int     not null,
        by_jnum                 int     not null,
        PRIMARY KEY(image_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'image_key' : ('image', 'image_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the image flower, providing pre-computed sorts for images',
        Table.COLUMN : {
                'image_key' : 'identifies the image',
                'by_default' : 'default sort for the image (Expression, Phenotype, and Molecular images uses different sorting rules.  They are encapsulated in this field.',
                'by_jnum' : 'sort by J: number for the reference of the image',
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
