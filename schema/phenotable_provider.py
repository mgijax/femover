#!./python

import Table

# maps from genotypes (in phenotable_to_genotype) to the various providers
# (in phenotable_center) of annotations for those genotypes

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'phenotable_provider'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key                      int             not null,
        phenotable_genotype_key         int             not null,
        phenotyping_center_key          int     null,
        interpretation_center_key       int     null,
        sequence_num                    int     null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'genotype_key' : 'create index %s on %s (phenotable_genotype_key)',
        'pc_key' : 'create index %s on %s (phenotyping_center_key)',
        'ic_key' : 'create index %s on %s (interpretation_center_key)',
}

keys = {
        'phenotyping_center_key' : ('phenotable_center', 'center_key'),
        'interpretation_center_key' : ('phenotable_center', 'center_key'),
}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'includes pairs of providers for annotations to genotypes (used for the allele and genotype detail pages); there can be multiple rows per genotype',
        Table.COLUMN : {
                'unique_key' : 'unique key identifying this row',
                'phenotable_genotype_key' : 'key for corresponding genotype object',
                'phenotyping_center_key' : 'identifies the provider serving as phenotyping center',
                'interpretation_center_key' : 'identifies the provider serving as data interpretation center',
                'sequence_num' : 'used to order the providers for a given genotype',
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
