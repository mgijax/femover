#!/usr/local/bin/python

# Purpose: print the database connection parameters for the target database

import sys
if '.' not in sys.path:
	sys.path.insert (0, '.')

import config

USAGE = 'Usage: %s [ -t | -h | -d | -u | -p ]' % sys.argv[0]

###--- Main program ---###

if len(sys.argv) > 2:		# if no SQL, just bail out as a no-op
	raise Error, 'At most one command-line parameter is allowed'

if len(sys.argv) == 1:
	print '%s %s %s %s %s' % (config.TARGET_TYPE, config.TARGET_HOST,
		config.TARGET_DATABASE, config.TARGET_USER,
		config.TARGET_PASSWORD)
elif sys.argv[1] == '-t':
	print config.TARGET_TYPE
elif sys.argv[1] == '-h':
	print config.TARGET_HOST
elif sys.argv[1] == '-d':
	print config.TARGET_DATABASE
elif sys.argv[1] == '-u':
	print config.TARGET_USER
elif sys.argv[1] == '-p':
	print config.TARGET_PASSWORD
