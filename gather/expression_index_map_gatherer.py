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
		cols1 = [ 'assay_type', 'full_coding_assay_type', 'seqNum' ]
		rows1 = [
			('Immunohistochemistry (section)',
				'Immunohistochemistry', 1),
			('In situ RNA (section)', 'RNA in situ', 2),
			('Immunohistochemistry (whole mount)',
				'Immunohistochemistry', 3),
			('In situ RNA (whole mount)', 'RNA in situ', 4),
			('In situ reporter (knock in)',
				'In situ reporter (knock in)', 5),
			('Northern blot', 'Northern blot', 6),
			('Western blot', 'Western blot', 7),
			('RT-PCR', 'RT-PCR', 8),
			('cDNA clones', None, 9),
			('RNAse protection', 'RNase protection', 10),
			('Nuclease S1', 'Nuclease S1', 11),
			('Primer Extension', None, 12),
			]

		self.output.append ( (cols1, rows1) )

		cols2 = [ 'age_string', 'min_theiler_stage',
				'max_theiler_stage', 'seqNum' ]
		rows2 = [ ('0.5', 1, 1, 1),
			('1', 1, 3, 2),
			('1.5', 1, 3, 3),
			('2', 1, 4, 4),
			('2.5', 1, 4, 5),
			('3', 3, 5, 6),
			('3.5', 3, 5, 7),
			('4', 4, 6, 8),
			('4.5', 5, 7, 9),
			('5', 5, 8, 10),
			('5.5', 5, 8, 11),
			('6', 7, 8, 12),
			('6.5', 8, 10, 13),
			('7', 9, 10, 14),
			('7.5', 9, 12, 15),
			('8', 10, 13, 16),
			('8.5', 12, 14, 17),
			('9', 12, 15, 18),
			('9.5', 13, 16, 19),
			('10', 14, 17, 20),
			('10.5', 15, 18, 21),
			('11', 16, 19, 22),
			('11.5', 17, 20, 23),
			('12', 19, 20, 24),
			('12.5', 19, 21, 25),
			('13', 20, 21, 26),
			('13.5', 21, 22, 27),
			('14', 21, 22, 28),
			('14.5', 22, 22, 29),
			('15', 22, 23, 30),
			('15.5', 23, 23, 31),
			('16', 23, 24, 32),
			('16.5', 24, 24, 33),
			('17', 24, 25, 34),
			('17.5', 25, 25, 35),
			('18', 25, 26, 36),
			('18.5', 26, 26, 37),
			('19', 26, 26, 38),
			('19.5', 26, 26, 39),
			('20', 26, 26, 40),
			('E', 1, 26, 41),
			('P', 28, 28, 42),
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
		[ 'assay_type', 'full_coding_assay_type', 'seqNum' ],
		'expression_index_assay_type_map'),

	('expression_index_age_map',
		[ 'age_string', 'min_theiler_stage', 'max_theiler_stage',
			'seqNum' ],
		'expression_index_age_map'),
	]

# global instance of a ExpressionIndexMapsGatherer
gatherer = ExpressionIndexMapsGatherer (files)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
