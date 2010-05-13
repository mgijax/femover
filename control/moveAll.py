#!/usr/local/bin/python

# execute all controllers, ensuring that each table is generated only
# once (even if multiple controllers know about it, as with join tables)

import config
import logger
import markerController
import referenceController
import sequenceController
import alleleController

tables = []		# list of strings; tables populated so far

# walk through the various controllers, executing each one
for package in [
    markerController, 
    referenceController, 
    sequenceController,
    alleleController
    ]:
	controller = package.controller

	# ensure that we do not rebuild any tables which were already built
	# by other controllers (eg- join tables)
	controller.removeTables (tables)

	# build this controller's tables, then add them to the list of ones
	# that were already built
	controller.go()
	tables = tables + controller.getTables()

	logger.debug ('finished %s, %d tables done' % (package.__name__,
		len(tables)) )

logger.info ('finished moveAll.py')
