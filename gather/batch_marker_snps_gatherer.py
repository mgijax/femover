#!/usr/local/bin/python
# 
# gathers data for the 'batch_marker_snps' table in the front-end database.
# Revised in March 2015 to customize and reduce memory footprint.

import Gatherer
import config
import MarkerSnpAssociations
import OutputFile
import dbAgnostic
import logger
import gc

###--- Globals ---###

cacheSize = 50000	# rough number of output rows to cache in memory

outFile = OutputFile.OutputFile('batch_marker_snps')	# output data file

###--- Functions ---###

def getChromosomes():
	# retrieve the ordered list of mouse chromosomes

	cmd0 = '''select chromosome
		from mrk_chromosome
		where _Organism_key = 1
		order by chromosome'''
	(cols, rows) = dbAgnostic.execute(cmd0)

	chromosomes = []	# list of mouse chromosomes
	for row in rows:
		chromosomes.append(row[0])
	logger.debug('Got %d mouse chromosomes' % len(chromosomes))
	return chromosomes

def getMarkers(chromosome):
	# get the markers for the given chromosome

	markers = []

	cmd1 = '''select distinct m._Marker_key
		from mrk_marker m
		where m._Organism_key = 1
			and m._Marker_Type_key != 6
			and not exists(select 1 from mrk_mcv_cache mcv where mcv._marker_key=m._marker_key and mcv._mcvterm_key=6238170)
			and m._Marker_Status_key in (1,3)
			and m.chromosome = '%s'
		order by m._Marker_key''' % chromosome
	(cols, rows) = dbAgnostic.execute(cmd1)

	for row in rows:
		markers.append(row[0])

	del rows
	gc.collect()

	logger.debug('Got %d markers on chromosome %s' % (len(markers),
		chromosome)) 
	return markers

def main():
	chromosomes = getChromosomes()

	outCols = [ Gatherer.AUTO, '_Marker_key', 'snpID' ]
	cols = [ '_Marker_key', 'snpID' ]
	rows = []

	# We walk through the markers of each chromosome, collecting the SNP
	# IDs for each marker, writing them out periodically to save space.

	for chrom in chromosomes:
		markers = getMarkers(chrom)

		for markerKey in markers:
			snps = MarkerSnpAssociations.getSnpIDs(markerKey, chrom)
			for snpID in snps:
				rows.append( (markerKey, snpID) )

			# Our cache size is approximate, so we check it only
			# after each marker, as this is close enough.

			if len(rows) >= cacheSize:
				outFile.writeToFile(outCols, cols, rows)
				rows = []
				gc.collect()

	# If any unwritten rows, we need to write them out.

	if rows:
		outFile.writeToFile(outCols, cols, rows)
		rows = []
		gc.collect() 

	outFile.close()
	print '%s %s' % (outFile.getPath(), 'batch_marker_snps')
	return

if __name__ == '__main__':
	main()
