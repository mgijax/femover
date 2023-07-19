#!./python

import Table

# contains data definition information for the recombinase_allele_imagepane
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'recombinase_allele_system_to_image'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        allele_system_key       int             not null,
        image_key               int             not null,
        sequence_num            int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
        'allele_system_key' : ('recombinase_allele_system', 'allele_system_key'),
        'image_key' : ('image', 'image_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('allele_system_key',
        'create index %s on %s (allele_system_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between allele/system pairs and their associated images',
        Table.COLUMN : {
                'unique_key' : 'unique identifier for this record, no other significance',
                'allele_system_key' : 'identifies the allele/system pair',
                'image_key' : 'identifies the associated image',
                'sequence_num' : 'used to order images for an allele/system pair',
                },
        Table.INDEX : {
                'allele_system_key' : 'clusters rows so that records involving the same allele/system pair are grouped together on disk, to aid efficiency',
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
