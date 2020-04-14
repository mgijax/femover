#!./python

import Table

# contains data definition information for the sequence table

###--- Globals ---###

# name of this database table
tableName = 'sequence'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        sequence_key    int             NOT NULL,
        sequence_type   text    NOT NULL,
        quality         text    NULL,
        status          text    NOT NULL,
        provider        text    NOT NULL,
        organism        text    NOT NULL,
        length          int             NULL,
        description     text    NULL,
        version         text    NULL,
        division        text    NULL,
        is_virtual      int             NOT NULL,
        has_clone_collection    int     NOT NULL,
        sequence_date   text    NULL,
        record_date     text    NULL,
        primary_id      text    NULL,
        logical_db      text    NULL,
        library         text    NULL,
        PRIMARY KEY(sequence_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'primary_id' : 'create index %s on %s (primary_id)',
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'central table of the sequence flower, containing the core data for each DNA, RNA, and polypeptide sequence',
        Table.COLUMN : {
                'sequence_key' : 'unique identifier for this sequence, same as _Sequence_key in mgd',
                'sequence_type' : 'type of sequence (RNA, DNA, Polypeptide, Not Loaded)',
                'quality' : 'quality of the sequence (how reliable is it?)',
                'status' : 'status of the sequence (ACTIVE, SPLIT, DELETED, Not Loaded)',
                'provider' : 'sequence provider (source of the sequence record)',
                'organism' : 'type of organism in which the sequence originated',
                'length' : 'number of letters in the sequence',
                'description' : 'describes the sequence',
                'version' : 'version number for the sequence',
                'division' : 'GenBank division containing this sequence (for GenBank sequences)',
                'is_virtual' : '1 if this is a virtual (consensus) sequence, 0 if an actual one',
                'has_clone_collection' : '1 if this sequence has any associated clone collections, 0 if not',
                'sequence_date' : 'last modification date of the sequence itself',
                'record_date' : 'last modification date of the sequence record from the provider',
                'primary_id' : 'primary accession ID for the sequence',
                'logical_db' : 'provider of the accession ID',
                'library' : 'name of the library',
                },
        Table.INDEX : {
                'primary_id' : 'quick lookup of sequences by primary ID',
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
