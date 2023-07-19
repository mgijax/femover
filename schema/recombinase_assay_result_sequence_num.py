#!./python

import Table

# contains data definition information for the
# recombinase_assay_result_sequence_num table

###--- Globals ---###

# name of this database table
tableName = 'recombinase_assay_result_sequence_num'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        result_key              int             not null,
        by_structure            int             not null,
        by_age                  int             not null,
        by_level                int             not null,
        by_pattern              int             not null,
        by_jnum_id              int             not null,
        by_assay_type           int             not null,
        by_reporter_gene        int             not null,
        by_detection_method     int             not null,
        by_assay_note           int             not null,
        by_allelic_composition  int             not null,
        by_sex                  int             not null,
        by_specimen_note        int             not null,
        by_result_note          int             not null,
        PRIMARY KEY(result_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'result_key' : ('recombinase_assay_result', 'result_key'), }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table within the recombinase flower, providing precomputed sorts for assay results (stored in another petal table)',
        Table.COLUMN : {
                'result_key' : 'identifies the recombinase assay result',
                'by_structure' : 'sort by printName of the anatomical structure',
                'by_age' : 'sort by specimen age',
                'by_level' : 'sort by detection level',
                'by_pattern' : 'sort by pattern of detection',
                'by_jnum_id' : 'sort by J: number for the reference',
                'by_assay_type' : 'sort by assay type',
                'by_reporter_gene' : 'sort by reporter gene',
                'by_detection_method' : 'sort by detection method',
                'by_assay_note' : 'sort by assay note',
                'by_allelic_composition' : 'sort by mutant alleles in the genetic background',
                'by_sex' : 'sort by gender of the specimen',
                'by_specimen_note' : 'sort by specimen note',
                'by_result_note' : 'sort by result note',
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
