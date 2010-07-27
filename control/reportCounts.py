#!/usr/local/bin/python

# Name: reportCounts.py
# Purpose: to report counts of rows for each table existing in the target
#	database
# Notes: database connection information comes from config.py

import sys
import time
import config
import dbManager	# for Postgres/MySQL access

USAGE = '''Usage: %s
	Report row counts for all tables in the target database defined in
	the config.py file.
''' % sys.argv[0]

###--- Globals ---###

DEBUG = False			# write debugging messages to stderr?
DBM = None			# used for managing the Postgres connection
START_TIME = time.time()	# integer time (sec) at script start time

###--- Functions ---###

def bailout (s, showUsage = True):
	if showUsage:
		sys.stderr.write (USAGE + '\n')
	sys.stderr.write ('Error: %s\n' % s)
	sys.exit(1)

def debug (s):
	if DEBUG:
		now = time.time()
		sys.stderr.write ('%8.3f : %s\n' % (now - START_TIME, s))
	return

def getTables():
	global DBM

	if config.TARGET_TYPE == 'postgres':
		DBM = dbManager.postgresManager (config.TARGET_HOST,
			config.TARGET_DATABASE, config.TARGET_USER,
			config.TARGET_PASSWORD)

		cmd = '''select TABLE_NAME
			from information_schema.tables
			where table_type='BASE TABLE'
				and table_schema='public'
			order by TABLE_NAME'''
	else:
		DBM = dbManager.mysqlManager (config.TARGET_HOST,
			config.TARGET_DATABASE, config.TARGET_USER,
			config.TARGET_PASSWORD)

		cmd = 'show tables'

	(columns, rows) = DBM.execute (cmd)

	tables = []
	for row in rows:
		tables.append (row[0])

	debug ('Got %d table names from %s' % (len(tables),
		config.TARGET_TYPE))
	return tables

def getCounts(tables):
	cmd = 'select count(1) from %s'

	counts = {}
	for tableName in tables:
		(columns, rows) = DBM.execute (cmd % tableName)
		counts[tableName] = rows[0][0]
	debug ('Got counts for %d tables from %s' % (len(counts),
		config.TARGET_TYPE))
	return counts

def addCommas (i):
	if i < 1000:
		return str(i)
	s = str(i % 1000)
	if len(s) < 3:
		s = '0' * (3 - len(s)) + s
	return addCommas(i / 1000) + ',' + s

def report (tables, counts):
	tables.sort()

	# build table heading row

	headings = [ 'Table', 'Rows' ]

	rows = [ headings ]

	# go through and collect a row of data for each table

	for tableName in tables:
		row = [ tableName, addCommas (counts[tableName]) ]
		rows.append (row)

	# find the maximum length for each column

	maxes = [ 0 ] * len(rows[0])

	for row in rows:
		i = 0
		while i < len(row):
			maxes[i] = max(maxes[i], len(row[i]))
			i = i + 1

	# write the output, left-justifying the table name and right-
	# justifying everything else

	print 'Row Counts for %s : %s' % (config.TARGET_TYPE, 
		config.TARGET_DATABASE)
	print

	for row in rows:
		print row[0].ljust(maxes[0]),

		i = 1
		while i < len(row):
			print row[i].rjust(maxes[i]),
			i = i + 1
		print
	return

def main():
	tables = getTables()
	report (tables, getCounts(tables))
	return

if __name__ == '__main__':
	main()
