#!./python

import Table

# contains MP system information for the allele summary table 

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_summary_disease'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        allele_disease_key              int             not null,
        allele_key              int             not null,
        disease         text            null,
        do_id           text            null,
        term_key int            not null,
        PRIMARY KEY(allele_disease_key))''' % tableName

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
        Table.TABLE : 'represents the Non-Not diseases displayed on summary page',
        Table.COLUMN : {
                'allele_disease_key' : 'unique key identifying this row',
                'allele_key' : 'key for the allele',
                'disease' : 'Non-Not disease for allele',
                'do_id' : 'ID from DO',
                'term_key' : 'Term Key from DO',
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
