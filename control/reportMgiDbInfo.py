#!/usr/local/bin/python

# Purpose: dump the contents of the MGI_dbInfo table from the source database

import sys
import config
import dbManager

###--- Globals ---###

Error = 'reportMgiDbInfo.error'

###--- Main program ---###

if len(sys.argv) > 1:		# if no SQL, just bail out as a no-op
	raise Error, 'No command-line parameters are allowed'

cmd = 'select * from MGI_dbInfo'

# get a dbManager to use in executing our SQL

if config.SOURCE_TYPE == 'mysql':
	dbMgr = dbManager.mysqlManager (config.SOURCE_HOST,
		config.SOURCE_DATABASE, config.SOURCE_USER,
		config.SOURCE_PASSWORD)
elif config.SOURCE_TYPE == 'postgres':
	dbMgr = dbManager.postgresManager (config.SOURCE_HOST,
		config.SOURCE_DATABASE, config.SOURCE_USER,
		config.SOURCE_PASSWORD)
else:
	raise Error, 'Unknown value for config.SOURCE_TYPE'

# execute the SQL and commit any changes

(columns, rows) = dbMgr.execute (cmd)
dbMgr.commit()

if len(rows) > 0:
	i = 0
	for column in columns:
		print '%s %s' % (column, rows[0][i])
		i = i + 1
