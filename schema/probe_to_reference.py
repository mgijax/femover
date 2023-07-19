#!./python

import Table

# contains data definition information for the probe_to_reference table

###--- Globals ---###

# name of this database table
tableName = 'probe_to_reference'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        probe_reference_key             int             NOT NULL,
        probe_key                               int             NOT NULL,
        reference_key                   int             NOT NULL,
        has_restriction_map             int             NOT NULL,
        has_sequences                   int             NOT NULL,
        has_aliases                             int             NOT NULL,
        has_polymorphisms               int             NOT NULL,
        note                                    text    NULL,
        qualifier                               text    NULL,
        PRIMARY KEY(probe_reference_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'reference_key' : 'create index %s on %s (reference_key)',
        'probe_key' : 'create index %s on %s (probe_key)',
        }

keys = {
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('probe_reference_key', 'create index %s on %s (probe_key, reference_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between probes and references',
        Table.COLUMN : {
                'probe_reference_key' : 'identifies this probe/reference pair; same as prb_reference._Reference_key in prod',
                'probe_key' : 'identifies the probe',
                'reference_key' : 'identifies the reference',
                'has_restriction_map' : '1 if reference has restriction map info, 0 if not',
                'has_sequences' : '1 if reference associates sequences with probe, 0 if not',
                'has_aliases' : '1 if reference has any aliases for probe, 0 if not',
                'has_polymorphisms' : '1 if reference has PCR/RFLV polymorphism data for probe, 0 if not',
                'note' : 'note for probe/reference pair',
                'qualifier' : 'qualifier describing this association',
                },
        Table.INDEX : {
                'probe_key' : 'clusters data so that all reference associations for a probe will be near each other on disk, aiding quick access',
                'reference_key' : 'look up all markers for a given reference',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
