#!./python

import Table

# contains disease model information for the disease summary grid

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'diseasetable_disease'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        diseasetable_disease_key                int             not null,
        allele_key              int             not null,
        disease         text    null,
        term_key                        int     null,
        disease_seq                     int     null,
        disease_id              text    null,
        is_header                       int     null,
        PRIMARY KEY(diseasetable_disease_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'allele_key' : 'create index %s on %s (allele_key)',
}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'represents the data needed to render disease information on the allele and genotype detail pages ',
        Table.COLUMN : {
                'diseasetable_disease_key' : 'unique key identifying this row',
                'allele_key' : 'key for the corresponding allele',
                'disease' : 'an Disease term',
                'term_key' : 'term key for the disease',
                'disease_seq' : 'alpha sort',
                'disease_id' : 'an Disease term ID',
                'is_header' : '1 or 0',
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
