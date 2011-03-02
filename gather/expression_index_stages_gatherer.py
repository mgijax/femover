#!/usr/local/bin/python
# 
# gathers data for the 'expression_index_stages' table in the front-end db

import Gatherer
import logger

###--- Classes ---###

class ExpressionIndexStagesGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the expression_index_stages table
	# Has: queries to execute against the source database
	# Does: queries the source database for assay types and ages for GXD
	#	literature index records, collates results, writes
	#	tab-delimited text file

	def postprocessResults (self):
		self.convertFinalResultsToList()

		ageCol = Gatherer.columnNumber(self.finalColumns, 'ageString')
		maxAge = 0.0

		# find the largest age, so we know how large the adult age
		# needs to be

		for row in self.finalResults:
			try:
				age = float(row[ageCol])
				if age > maxAge:
					maxAge = age
			except:
				pass

		logger.debug ('Found max age of %0.1f' % maxAge)

		adultAge = maxAge + 1.0

		# now assign the float ages

		for row in self.finalResults:
			try:
				age = float(row[ageCol])
			except:
				age = adultAge

			self.addColumn ('age', age, row, self.finalColumns)
		return

###--- globals ---###

cmds = [
	'''select s._Index_key, t.note as assayType, a.term as ageString
		from gxd_index_stages s,
			voc_text t,
			voc_term a
		where s._IndexAssay_key = t._Term_key
			and t.sequenceNum = 1
			and s._StageID_key = a._Term_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Index_key', 'assayType', 'age', 'ageString'
	]

# prefix for the filename of the output file
filenamePrefix = 'expression_index_stages'

# global instance of a ExpressionIndexStagesGatherer
gatherer = ExpressionIndexStagesGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
