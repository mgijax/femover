#!/usr/local/bin/python

import Table

# contains data definition information for the allele_recombinase_systems
# table

###--- Globals ---###

# name of this database table
tableName = 'allele_recombinase_systems'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	allele_key			int		not null,
	in_adipose_tissue		int		null,
	in_alimentary_system		int		null,
	in_branchial_arches		int		null,
	in_cardiovascular_system	int		null,
	in_cavities_and_linings		int		null,
	in_endocrine_system		int		null,
	in_head				int		null,
	in_hemolymphoid_system		int		null,
	in_integumental_system		int		null,
	in_limbs			int		null,
	in_liver_and_biliary_system	int		null,
	in_mesenchyme			int		null,
	in_muscle			int		null,
	in_nervous_system		int		null,
	in_renal_and_urinary_system	int		null,
	in_reproductive_system		int		null,
	in_respiratory_system		int		null,
	in_sensory_organs		int		null,
	in_skeletal_system		int		null,
	in_tail				int		null,
	in_early_embryo			int		null,
	in_extraembryonic_component	int		null,
	in_embryo_other			int		null,
	in_postnatal_other		int		null,
	detected_count			int		null,
	not_detected_count		int		null,
	PRIMARY KEY(allele_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'allele_key' : ('allele', 'allele_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the allele flower, containing summary data for recombinase activity of each allele',
	Table.COLUMN : {
		'allele_key' : 'identifies the allele',
		'in_adipose_tissue' : '1 if activity in adipose tissue, 0 if not',
		'in_alimentary_system' : '1 if activity in the alimentary system, 0 if not',
		'in_branchial_arches' : '1 if activity in the branchial arches, 0 if not',
		'in_cardiovascular_system' : '1 if activity in the cardiovascular system, 0 if not',
		'in_cavities_and_linings' : '1 if activity in cavities and linings, 0 if not',
		'in_endocrine_system' : '1 if activity in the endocrine system, 0 if not',
		'in_head' : '1 if activity in the head, 0 if not',
		'in_hemolymphoid_system' : '1 if activity in the hemolymphoid system, 0 if not',
		'in_integumental_system' : '1 if activity in the integumental system, 0 if not',
		'in_limbs' : '1 if activity in the limbs, 0 if not',
		'in_liver_and_biliary_system' : '1 if activity in the liver and biliary system, 0 if not',
		'in_mesenchyme' : '1 if activity in mesenchyme, 0 if not',
		'in_muscle' : '1 if activity in muscle, 0 if not',
		'in_nervous_system' : '1 if activity in the nervous system, 0 if not',
		'in_renal_and_urinary_system' : '1 if activity in the renal and urinary system, 0 if not',
		'in_reproductive_system' : '1 if activity in the reproductive system, 0 if not',
		'in_respiratory_system' : '1 if activity in the respiratory system, 0 if not',
		'in_sensory_organs' : '1 if activity in the sensory organs, 0 if not',
		'in_skeletal_system' : '1 if activity in the skeletal system, 0 if not',
		'in_tail' : '1 if activity in the tail, 0 if not',
		'in_early_embryo' : '1 if activity in the early embryo, 0 if not',
		'in_extraembryonic_component' : '1 if activity in the extraembryonic component, 0 if not',
		'in_embryo_other' : '1 if activity in other parts of the embryo, 0 if not',
		'in_postnatal_other' : '1 if activity in other postnatal parts, 0 if not',
		'detected_count' : 'count of systems where recombinase activity was detected',
		'not_detected_count' : 'count of systems where recombinase activity was studied and was not detected',
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
