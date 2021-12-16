#!./python

import Table

# contains data definition info for the recombinase_assay_result table

###--- Globals ---###

# name of this database table
tableName = 'recombinase_assay_result'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        result_key              int             not null,
        allele_system_key       int             not null,
        structure_key   int             null,
        structure               text    null,
        age                     text    null,
        level                   text    null,
        pattern                 text    null,
        jnum_id                 text    null,
        assay_type              text    null,
        reporter_gene           text    null,
        detection_method        text    null,
        sex                     text    null,
        allelic_composition     text    null,
        background_strain       text    null,
        assay_note              text    null,
        result_note             text    null,
        specimen_note           text    null,
        probe_id                text    null,
        probe_name              text    null,
        antibody_id             text    null,
        antibody_name           text    null,
        cell_types              text    null,
        PRIMARY KEY(result_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'structure_key' : 'create index %s on %s (structure_key)',
        }

keys = {
        'allele_system_key' : ('recombinase_allele_system', 'allele_system_key'),
        'structure_key' : ('term', 'term_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('allele_system_key',
        'create index %s on %s (allele_system_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the recombinase flower, containing detailed data for each recombinase assay result',
        Table.COLUMN : {
                'result_key' : 'unique identifier for this record',
                'allele_system_key' : 'identifies the allele/system pair',
                'structure_key' : 'term key for anatomical structure (stage-specific / EMAPS)',
                'structure' : 'anatomical structure',
                'age' : 'age of the specimen',
                'level' : 'level of activity detected',
                'pattern' : 'pattern of the activity',
                'jnum_id' : 'J: number ID for the reference, cached from the reference table',
                'assay_type' : 'type of assay employed',
                'reporter_gene' : 'reporter gene used',
                'detection_method' : 'detection method',
                'sex' : 'gender of the specimen',
                'allelic_composition' : 'mutant alleles in the specimen',
                'background_strain' : 'mouse strain used as a background',
                'assay_note' : 'note on the assay',
                'result_note' : 'note on this individual result',
                'specimen_note' : 'note on the specimen',
                'probe_id' : 'primary ID for the probe',
                'probe_name' : 'name of the probe',
                'antibody_id' : 'primary ID of the antibody',
                'antibody_name' : 'name of the antibody',
                'cell_types' : 'comma-separated list of cell types',
                },
        Table.INDEX : {
                'allele_system_key' : 'clusters data so all results for an allele/system pair are stored on disk together, to aid efficiency',
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
