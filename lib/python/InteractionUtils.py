# Module: InteractionUtils.py
# Purpose: to provide extra functions for detail with marker interactions

import dbAgnostic
import logger

###--- Functions ---###

def getMarkerInteractionCounts():
	# Purpose: get the number of other markers that interact with each
	#	marker
	# Returns: dictionary; maps marker_key to a count of other markers
	#	that interact with that one

	# need to compile a temp table including each marker in marker1 and
	# each marker that interacts with it in marker2

	dbAgnostic.execute('''create temp table markers (
		marker1 int not null,
		marker2 int not null)''')

	logger.debug('created temp table for interactions')

	dbAgnostic.execute('''insert into markers
		select distinct _Object_key_1, _Object_key_2
		from mgi_relationship
		where _Category_key = 1001''')
	
	logger.debug('added forward relationships to temp table')

	# note that there will be some redundancy introduced here for markers
	# which both interact with each other.  That's okay, as we'll remove
	# it with a 'distinct' later on.

	dbAgnostic.execute('''insert into markers
		select distinct _Object_key_2, _Object_key_1
		from mgi_relationship
		where _Category_key = 1001''')

	logger.debug('added reverse relationships to temp table')

	# This temp table will grow very larger, so let's add an index and
	# then cluster the data, to make computing the counts by each marker
	# in marker1 easy.

	dbAgnostic.execute('create index markers_idx on markers (marker1)')

	logger.debug('added index to temp table')

	dbAgnostic.execute('cluster markers using markers_idx')

	logger.debug('clustered data in temp table')

	cols, rows = dbAgnostic.execute('''
		select marker1, count(distinct marker2) as ct
		from markers
		group by 1''')

	marker1Col = dbAgnostic.columnNumber(cols, 'marker1')
	ctCol = dbAgnostic.columnNumber(cols, 'ct')

	d = {}
	
	for row in rows:
		d[row[marker1Col]] = row[ctCol]

	logger.debug('extracted %d interaction counts' % len(d))
	return d
