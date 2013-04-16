# Name: utils.py
# Purpose: catchall for miscellaneous utility functions that can be shared
#	across gatherers

def cleanupOrganism (organism):
	# translate raw 'organism' value from the database to a version
	# preferred for front-end display purposes

	if organism == 'mouse, laboratory':
		return 'mouse'
	if organism == 'dog, domestic':
		return 'dog'
	return organism
