#!./python

# Purpose: accept a SQL on the command line and execute it against the db

import sys
import config
import dbManager

###--- Globals ---###

error = 'dbExecute.error'

###--- Main program ---###

if len(sys.argv) == 1:          # if no SQL, just bail out as a no-op
        sys.exit(0)

# if we received the SQL as multiple parameters, join the back together.
# or if it's just a single string, then grab it.

if len(sys.argv) > 2:
        cmd = ' '.join (sys.argv[1:])
else:
        cmd = sys.argv[1]

# trim off any enclosing quotes around the SQL command

if cmd[0] == "'" and cmd[-1] == "'":
        cmd = cmd[1:-1]
elif cmd[0] == '"' and cmd[-1] == '"':
        cmd = cmd[1:-1]

# get a dbManager to use in executing our SQL

if config.TARGET_TYPE == 'postgres':
        dbMgr = dbManager.postgresManager (config.TARGET_HOST,
                config.TARGET_DATABASE, config.TARGET_USER,
                config.TARGET_PASSWORD)
else:
        raise Exception('%s: Unknown value for config.TARGET_TYPE' % error)

# execute the SQL and commit any changes

results = dbMgr.execute (cmd)
dbMgr.commit()

# if there were rows returned, print them

if results and results[0]:
        print(str(results))
