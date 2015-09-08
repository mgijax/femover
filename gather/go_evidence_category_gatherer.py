#!/usr/local/bin/python
# 
# gathers data for the 'go_evidence_category' table in the front-end database

import Gatherer

###--- Globals ---###

EXPERIMENTAL = 'Experimental'
HOMOLOGY = 'Homology'
AUTOMATED = 'Automated'
OTHER = 'Other'

EXPERIMENTAL_CODES = [ 'EXP', 'IDA', 'IMP', 'IPI', 'IGI', 'IEP' ]
HOMOLOGY_CODES = [ 'IBA', 'IKR', 'ISS', 'ISM', 'ISO', 'ISA', 'IAS', 'IBD',
	'IMR', 'IRD' ]
AUTOMATED_CODES = [ 'IEA', 'RCA' ]
OTHER_CODES = [ 'IC', 'TAS', 'NAS', 'ND' ]

CATEGORIES = [
	(EXPERIMENTAL_CODES, EXPERIMENTAL),
	(HOMOLOGY_CODES, HOMOLOGY),
	(AUTOMATED_CODES, AUTOMATED),

	# Other is the default, so we don't really need to check for it.
	#(OTHER_CODES, OTHER),
	]

###--- Classes ---###

class GoEvidenceCategoryGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the go_evidence_category table
	# Has: queries to execute against the source database
	# Does: queries the source database for GO Evidence Codes, assigns 
	#	them to categories, writes tab-delimited text file

	def collateResults(self):
		cols, rows = self.results[0]

		self.finalColumns = [ 'evidence_code', 'evidence_category' ]
		self.finalResults = []

		for row in rows:
			abbrev = row[0]
			category = OTHER

			for (codes, codesCategory) in CATEGORIES:
				if abbrev in codes:
					category = codesCategory
					break

			self.finalResults.append( [ abbrev, category ] )
		return

###--- globals ---###

cmds = [
	'''select t.abbreviation
	from voc_term t, voc_vocab v
	where t._Vocab_key = v._Vocab_key
	and v.name = 'GO Evidence Codes' ''' 
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ 'evidence_code', 'evidence_category' ]

# prefix for the filename of the output file
filenamePrefix = 'go_evidence_category'

# global instance of a GoEvidenceCategoryGatherer
gatherer = GoEvidenceCategoryGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
