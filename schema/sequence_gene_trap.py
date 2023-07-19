#!./python

import Table

# contains data definition information for the sequence_gene_trap table

###--- Globals ---###

# name of this database table
tableName = 'sequence_gene_trap'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        sequence_key            int             NOT NULL,
        tag_method              text    NOT NULL,
        vector_end              text    NOT NULL,
        reverse_complement      text    NOT NULL,
        good_hit_count          int             NULL,
        point_coordinate        float           NULL,
        PRIMARY KEY(sequence_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'sequence_key' : ('sequence', 'sequence_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the sequence flower, containing extra data for gene trap sequences',
        Table.COLUMN : {
                'sequence_key' : 'identifies the sequence',
                'tag_method' : 'name of the procedure used to produce a sequence tag from a gene trap insertion in a mutant cell line',
                'vector_end' : 'vector end (upstream, downstream, Not Specified, Not Applicable)',
                'reverse_complement' : 'yes if the sequence is reverse-complemented, no if not',
                'good_hit_count' : 'number of good blat hits',
                'point_coordinate' : 'single coordinate picked to indicate where the insertion actually happened',
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
