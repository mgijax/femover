#!/usr/local/bin/python
# 
# gathers data for the 'expression_index_*_map' table in the front-end database

import Gatherer

###--- Classes ---###

class ExpressionIndexMapsGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the expression_index_age_map and
	#	expression_index_assay_type_map tables
	# Has: knowledge of the index/full-coding mappings for ages and assay
	#	types
	# Does: writes out the mapping of stages and assay types for the
	#	mapping tables

	def collateResults (self):
		cols1 = [ 'assay_type', 'full_coding_assay_type' ]
		rows1 = [
			('Prot-sxn', 'Immunohistochemistry'),
			('RNA-sxn', 'RNA in situ'),
			('Prot-WM', 'Immunohistochemistry'),
			('RNA-WM', 'RNA in situ'),
			('Knock in', 'In situ reporter (knock in)'),
			('Northern', 'Northern blot'),
			('Western', 'Western blot'),
			('RT-PCR', 'RT-PCR'),
#			('cDNA', ''),
			('RNAse prot', 'RNase protection'),
			('S1 nuc', 'Nuclease S1'),
#			('Primer ex', ''),
			]

		self.output.append ( (cols1, rows1) )

		cols2 = [ 'age_string', 'min_theiler_stage',
				'max_theiler_stage' ]
		rows2 = [ ('0.5', 1, 1),
			('1', 1, 3),
			('1.5', 1, 3),
			('2', 1, 4),
			('2.5', 1, 4),
			('3', 3, 5),
			('3.5', 3, 5),
			('4', 4, 6),
			('4.5', 5, 7),
			('5', 5, 8),
			('5.5', 5, 8),
			('6', 7, 8),
			('6.5', 8, 10),
			('7', 9, 10),
			('7.5', 9, 12),
			('8', 10, 13),
			('8.5', 12, 14),
			('9', 12, 15),
			('9.5', 13, 16),
			('10', 14, 17),
			('10.5', 15, 18),
			('11', 16, 19),
			('11.5', 17, 20),
			('12', 19, 20),
			('12.5', 19, 21),
			('13', 20, 21),
			('13.5', 21, 22),
			('14', 21, 22),
			('14.5', 22, 22),
			('15', 22, 23),
			('15.5', 23, 23),
			('16', 23, 24),
			('16.5', 24, 24),
			('17', 24, 25),
			('17.5', 25, 25),
			('18', 25, 26),
			('18.5', 26, 26),
			('19', 26, 26),
			('19.5', 26, 26),
			('20', 26, 26),
			('E', 1, 26),
			('A', 28, 28),
			]

		self.output.append ( (cols2, rows2) ) 
		return

###--- globals ---###

cmds = [
	# bogus command which we need to issue for the sake of the framework
	'select 1 from mgi_dbinfo',
	]

files = [
	('expression_index_assay_type_map',
		[ 'assay_type', 'full_coding_assay_type' ],
		'expression_index_assay_type_map'),

	('expression_index_age_map',
		[ 'age_string', 'min_theiler_stage', 'max_theiler_stage' ],
		'expression_index_age_map'),
	]

# global instance of a ExpressionIndexMapsGatherer
gatherer = ExpressionIndexMapsGatherer (files)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
