import config
import sys
import Dispatcher
import logger

###--- Globals ---###

gatherDir = config.GATHER_DIR	# string; path to the directory of Gatherers
populatorDir = config.POPULATE_DIR	# string; path to dir of Populators
maxSubprocs = config.MAX_SUBPROCESSES	# integer; max num of subprocs to use

###--- Functions ---###

def identifyFailures (
	dispatcher, 	# Dispatcher; handled execution of subprocesses
	tableMap	# dictionary; maps table name to dispatcher id
	):
	# Purpose: to check for failures during gathering or populating
	# Returns: 2-item tuple (list of strings (each a table name with a
	#	failure), count of tables which ran successfully)
	# Assumes: any IDs from the tableMap have finished running in the
	#	dispatcher
	# Modifies: nothing
	# Throws: nothing

	failed = []	# list of strings, each a table name with a failure
	okay = 0	# integer; count of tables which ran successfully

	for table in tableMap.keys():
		id = tableMap[table]
		
		# failure is indicated either by a non-zero return code, or
		# by presence of output in its stderr

		if dispatcher.getReturnCode(id):
			failed.append (table)
			logger.debug ('%s return code: %s' % (table,
				dispatcher.getReturnCode(id)))
			logger.debug ('%s stderr: %s' % (table,
				dispatcher.getStderr(id)))
#		elif dispatcher.getStderr(id):
#			failed.append (table)
#			logger.debug ('%s stderr: %s' % (table,
#				dispatcher.getStderr(id)))
		else:
			okay = okay + 1

	return failed, okay

def main (
	controller	# Controller object upon which to operate
	):
	# Purpose: generic main program for all Controller subclasses
	# Returns: nothing
	# Assumes: nothing
	# Modifies: writes a data file to the file system, loads it into a
	#	table in the MySQL database
	# Throws: propagates any exceptions

	# add a log entry that this script ran with its arguments
	logger.info (' '.join(sys.argv))

	# if there were no arguments on the command-line, then we fully
	# recreate and repopulate the table.  if there were arguments, then
	# the first specifies the key name and the second the key value for
	# which we need to refresh the rows in the table.

	if len(sys.argv) > 1:
		controller.go (sys.argv[1], int(sys.argv[2]))
	else:
		controller.go()
	return

###--- Classes ---###

class Controller:
	# Is: a controller that facilitates the running of gatherers and
	#	populators for a set of MySQL database tables
	# Has: a set of database tables to refresh
	# Does: executes in parallele the gatherers and populators for those
	#	database tables

	def __init__ (self,
		tables, 		# list of strings; names of tables
		subprocesses = maxSubprocs	# int; num of subprocs to use
		):
		# Purpose: constructor
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		self.tables = tables
		self.subprocesses = subprocesses
		return

	def getTables (self):
		# Purpose: return the list of tables that this controller is
		#	responsible for refreshing
		# Returns: list of strings
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		return self.tables[:]

	def removeTables (self,
		tables			# list of strings; names of tables
		):
		# Purpose: to remove the given tables from consideration in
		#	this controller object
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		for table in tables:
			if table in self.tables:
				self.tables.remove(table)
		return

	def go (self,
		keyField = None,	# string; name of key field
		keyValue = None		# integer; value of key field
		):
		# Purpose: to refresh the table's data, either fully or for a
		#	particular key.  (The default is to do all.  If both
		#	keyField and keyValue are specified, then the refresh
		#	is only for the specified values.)
		# Returns: nothing
		# Assumes: nothing
		# Modifies: runs gatherers and populators
		# Throws: propagates any exceptions
		# Notes: If any gatherers fail, then we abort without altering
		#	the tables at all.  If any populators fail, then the
		#	database will be left in an inconsistent state.

		# the Dispatcher will handle parallel execution of the
		# gatherers and populators.
		dispatcher = Dispatcher.Dispatcher(self.subprocesses)

		parms = ''			# command-line arguments
		if keyField and keyValue:
			parms = '%s %s' % (keyField, keyValue)

		# run gatherers in parallel

		tableMap = {}		# maps table name to dispatcher's ID
		for table in self.tables:
			path = gatherDir + table + "Gatherer.py " + parms
			logger.info ("scheduled: " + path.strip())
			tableMap[table] = dispatcher.schedule(path.strip())
		dispatcher.wait()
		logger.info ('Finished gatherers')

		# check for failures during gathering.  if there were any,
		# stop before doing any populating.

		failed, okay = identifyFailures (dispatcher, tableMap)

		logger.info ('Ran %d gatherers successfully' % okay)

		if failed:
			logger.error ('Failed gathering for table(s): %s' % \
				', '.join (failed))
			logger.error ('Exiting before running any Populators')
			sys.exit(-1)

		# run populators in parallel

		tableMap2 = {}		# maps table name to dispatcher's ID
		for table in self.tables:
			# Each gatherer will write to stdout the path to the
			# data file it wrote.  Pick it up and use it to
			# populate the corresponding table.

			filename = dispatcher.getStdout(tableMap[table])
			filename = filename[0].strip()
			if not filename:
				continue	# no data, no populating

			cmd = populatorDir + table \
				+ "Populator.py " + filename + " " + parms
			cmd = cmd.strip()
			logger.info ("scheduled: " + cmd)
			id = dispatcher.schedule(cmd)
			tableMap2[table] = id
		dispatcher.wait()
		logger.info ('Finished populators')

		# check for failures during populating

		failed, okay = identifyFailures (dispatcher, tableMap2)

		logger.info ('Ran %d populators successfully' % okay)

		if failed:
			logger.error ('Failed populating table(s): %s' % \
				', '.join (failed))
			logger.error ('Exiting with database in an ' + \
				'inconsistent state.')
			sys.exit(-2)
		return
