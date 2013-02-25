#!/usr/local/bin/python
# 
# Gathers data for the 'test_stats' table in the front-end database.
# Reads multiple user-defined queries designed to generate test data
# for comparison on the front-end, using automated tests.
#
#	Author: kstone 2013/02/14
#

import Gatherer
import logger
import dbAgnostic
import re
from TestData import *

# define which groups of test data we will actually run and load
import GXDTestData,GXDLitTestData,VocabTestData
TESTS = [GXDTestData,GXDLitTestData,VocabTestData]

SQL_VAR_PATTERN = "\$\{([a-zA-Z0-9]+)\}"
# compile the regex for parsing out variable strings in the SQL statements
sql_re = re.compile(SQL_VAR_PATTERN)

rows_by_id = {}

CIRCULAR_ERROR = "###CircularError####"
REFERENCE_ERROR = "###ReferenceError####"
EXISTING_ERROR = "###ExistingError###"

###--- Functions to be moved into a lib module ---###
# this function iterates the TestData SQLs and returns each row with the result data or error fields populated
def iterateSqls():
	all_rows = []
	# iterate all the tests first in order to map all the ids (necessary for resolving variables later on)
	for testData in TESTS:
		for test_sql in testData.Queries:
			id = test_sql[ID]
			# sanitise input
			test_sql[DESCRIPTION] = test_sql[DESCRIPTION].strip()
			test_sql[SQLSTATEMENT] = test_sql[SQLSTATEMENT].strip()
			# initialise some values
			test_sql[UPDATED] = False
			test_sql[ERROR] = ""
			test_sql[RETURNDATA] = ""
			test_sql[GROUP] = testData.__name__.replace("TestData","")
			rows_by_id[id] = test_sql
			all_rows.append(test_sql)
	# perform the sql statements
	for row in all_rows:
		run_sql_statement(row)
	#logger.debug(all_rows)
	return all_rows

# process SQL and perform any variable substitutions
def run_sql_statement(row,idStack=[]):
	global sql_re
	global rows_by_id

	if row[UPDATED]:
		if row[ERROR]:
			return (EXISTING_ERROR,"Referenced id %s has an error"%row[ID])
		return row[RETURNDATA]

	if row[ID] in idStack:
		error_text = "Circular Variable Path: %s references %s"%(idStack[-1],row[ID]) 
		return (CIRCULAR_ERROR,error_text) 

	idStack.append(row[ID])

	sqlstatement = row[SQLSTATEMENT]

	# find out if there are any variable substitutions that need to be made
	vars = sql_re.findall(sqlstatement)
	#print "processing vars: %s in %s"%(vars,row[ID].cell.text)
	if vars:
		for var in vars:
			if var not in rows_by_id:
				error_text = "Reference ID %s does not exist"%var
				return (REFERENCE_ERROR,error_text)
			return_value = run_sql_statement(rows_by_id[var],idStack)
			if CIRCULAR_ERROR in return_value:
				error_text = return_value[1]
				# there was an error. Clear the return data cell
				row[RETURNDATA] = ''
				row[ERROR] = error_text
				row[UPDATED] = True
				return return_value
			elif REFERENCE_ERROR in return_value:
				error_text = return_value[1]
				# there was an error. Clear the return data cell
				row[RETURNDATA] = ''
				row[ERROR] = error_text
				row[UPDATED] = True
				return return_value
			elif EXISTING_ERROR in return_value:
				error_text = return_value[1]
				# there was an error. Clear the return data cell
				row[RETURNDATA] = ''
				row[ERROR] = error_text
				row[UPDATED] = True
				return return_value
			else:
				# replace the variable in sqlstatement with return data
				pattern = "\$\{%s\}"%var
				#print "replacing with pattern %s"%pattern
				sqlstatement = re.sub(pattern,parse_sql_data(return_value),sqlstatement)

	logger.debug("running sql %s"%sqlstatement)
	data = ''
	try:
		(dbcols,dbrows) = dbAgnostic.execute(sqlstatement.strip())
		data = parse_sql_data(dbrows)
		logger.debug("ran SQL for %s. Got value %s"%(row[ID],data))
		row[RETURNDATA] = data
	except Exception, e:
		message = e[1]
		logger.debug(message)
		row[ERROR] = "SQL error: %s"%message
		#print "ran SQL for %s. Got error"%(row[ID].cell.text)
	row[UPDATED] = True
	return data

# do additional/special formatting of data before it gets written back to google
def parse_sql_data(inData):
	if len(inData) > 0:
		rowData = []
		for row in inData:
			if len(row) > 1:
				# we have multiple columns in result set, need to join them	
				rowData.append("ROW[%s]"%",".join(["%s"%x for x in row]))
			else:
				rowData.append("%s"%row[0])
		outData = ",".join(rowData)
	else:
		outData = inData
	
	return outData


###--- Classes ---###

class TestStatsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the test_stats table

	def collateResults(self):
		cols = [ '_test_stats', 'id', 'group_type', 'description', 'sql_statement','test_data','error']
		data_rows = iterateSqls()

		count = 0
		dbrows = []	
		for data in data_rows:
			count += 1
			dbrows.append( [count,data[ID],data[GROUP],data[DESCRIPTION],data[SQLSTATEMENT],data[RETURNDATA],data[ERROR]])
			
		self.finalColumns = cols
		self.finalResults = dbrows
		return

	def postprocessResults(self):
		return

###--- globals ---###

# The gatherer class becomes sad when there are no queries to execute at creation time
# This query won't actually be used by this code
cmds = ["""select 1 from mrk_marker limit 1"""] 

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_test_stats', 'id', 'group_type', 'description', 'sql_statement', 
        'test_data', 'error' ]

# prefix for the filename of the output file
filenamePrefix = 'test_stats'

# global instance of a AssayGatherer
gatherer = TestStatsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
