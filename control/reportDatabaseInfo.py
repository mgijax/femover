#!/usr/local/bin/python

# Name: reportCounts.py
# Purpose: to report counts of rows for each table existing in the target
#	database
# Notes: database connection information comes from config.py

import sys
import config
import dbManager	# for Postgres/MySQL access

USAGE = '''Usage: %s
	Report the contents of the database_info table in the target database.
''' % sys.argv[0]

###--- Functions ---###

def report (dbm = None):
	if dbm == None:
		if config.TARGET_TYPE == 'postgres':
			dbm = dbManager.postgresManager (config.TARGET_HOST,
				config.TARGET_DATABASE, config.TARGET_USER,
				config.TARGET_PASSWORD)
		else:
			dbm = dbManager.mysqlManager (config.TARGET_HOST,
				config.TARGET_DATABASE, config.TARGET_USER,
				config.TARGET_PASSWORD)

	(columns, rows) = dbm.execute (
		'select name, value from database_info order by unique_key')

	maxName = 0
	for row in rows:
		maxName = max (maxName, len(row[0]))

	for row in rows:
		print '%s : %s' % (row[0].ljust(maxName), row[1])
	return

if __name__ == '__main__':
	report()
