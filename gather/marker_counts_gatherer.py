#!/usr/local/bin/python
# 
# gathers data for the 'markerCounts' table in the front-end database

# NOTE: To add more counts:
#	1. add a fieldname for the count as a global (like ReferenceCount)
#	2. add a new query to 'cmds' in the main program
#	3. add processing for the new query to collateResults(), to tie the
#		query results to the new fieldname in each marker's dictionary
#	4. add the new fieldname to fieldOrder in the main program

import Gatherer
import logger
import config
import MarkerSnpAssociations
import GOFilter

###--- Globals ---###

ReferenceCount = 'referenceCount'
SequenceCount = 'sequenceCount'
AlleleCount = 'alleleCount'
GOCount = 'goTermCount'
OrthologCount = 'orthologCount'
GeneTrapCount = 'geneTrapCount'
GxdAssayCount = 'gxdAssayCount'
GxdResultCount = 'gxdResultCount'
GxdLiteratureCount = 'gxdLiteratureCount'
GxdTissueCount = 'gxdTissueCount'
GxdImageCount = 'gxdImageCount'
MappingCount = 'mappingCount'
SequenceRefSeqCount = 'seqRefSeqCount'
SequenceUniprotCount = 'seqUniprotCount'
CdnaSourceCount = 'cDNASourceCount'
MicroarrayCount = 'microarrayCount'
PhenotypeImageCount = 'phenotypeImageCount'
HumanDiseaseCount = 'humanDiseaseCount'
AllelesWithDiseaseCount = 'allelesWithHumanDiseasesCount'
AntibodyCount = 'antibodyCount'
SnpCount = 'snpCount'

error = 'markerCountsGatherer.error'

###--- Classes ---###

class MarkerCountsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerCounts table
	# Has: queries to execute against the source database
	# Does: queries the source database for marker counts,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# Purpose: to combine the results of the various queries into
		#	one single list of final results, with one row per
		#	marker

		# pre-process the GO annotation data

		goAnnot = []
		for key in GOFilter.getMarkerKeys():
			goAnnot.append ( (key,
				GOFilter.getAnnotationCount(key)) )

		goData = [ ['_Marker_key', 'numGO'], goAnnot ]
		logger.debug ('Pre-processed %d GO counts' % len(goAnnot))

		# list of count types (like field names)
		counts = []

		# initialize dictionary for collecting data per marker
		#	d[marker key] = { count type : count }
		d = {}
		for row in self.results[0][1]:
			d[row[0]] = {}

		# counts to add in this order, with each tuple being:
		#	(set of results, count constant, count column)

		toAdd = [ (self.results[1], ReferenceCount, 'numRef'),
			(self.results[2], SequenceCount, 'numSeq'),
			(self.results[3], AlleleCount, 'numAll'),
			(goData, GOCount, 'numGO'),
			(self.results[4], AntibodyCount, 'antibodyCount'),
			(self.results[5], GxdAssayCount, 'numAssay'),
			(self.results[6], OrthologCount, 'numOrtho'),
			(self.results[7], GeneTrapCount, 'numGeneTraps'),
			(self.results[8], GxdResultCount, 'resultCount'),
			(self.results[9], GxdLiteratureCount, 'indexCount'),
			(self.results[10], GxdTissueCount, 'tissueCount'),
			(self.results[11], GxdImageCount, 'paneCount'),
			(self.results[12], MappingCount, 'mappingCount'),
			(self.results[13], SequenceRefSeqCount, 'numSeq'),
			(self.results[14], SequenceUniprotCount, 'numSeq'),
			(self.results[15], CdnaSourceCount, 'cdnaCount'),
			(self.results[16], MicroarrayCount, 'affyCount'),
			(self.results[17], PhenotypeImageCount, 'imageCount'),
			(self.results[18], HumanDiseaseCount, 'diseaseCount'),
			(self.results[19], AllelesWithDiseaseCount,
				'alleleCount'),
			# SnpCount will be processed separately
			]

		for (r, countName, colName) in toAdd:
			logger.debug ('Processing %s, %d rows' % (countName,
				len(r[1])) )
			counts.append (countName)
			mrkKeyCol = Gatherer.columnNumber(r[0], '_Marker_key')
			countCol = Gatherer.columnNumber(r[0], colName)

			for row in r[1]:
				mrkKey = row[mrkKeyCol]
				if d.has_key(mrkKey):
					d[mrkKey][countName] = row[countCol]
				elif mrkKey == None:
					continue
				else:
					raise error, \
					'Unknown marker key: %d' % mrkKey

		# compile the count of SNPs associated with each marker and
		# add it to the counts in 'd'

		markerKeys = d.keys()
		markerKeys.sort()

		counts.append (SnpCount)

		for mrk in markerKeys:
			d[mrk][SnpCount] = MarkerSnpAssociations.getSnpCount(
				mrk)

		# compile the list of collated counts in self.finalResults
		self.finalResults = []
		self.finalColumns = [ '_Marker_key' ] + counts

		for markerKey in markerKeys:
			row = [ markerKey ]
			for count in counts:
				if d[markerKey].has_key (count):
					row.append (d[markerKey][count])
				else:
					row.append (0)

			self.finalResults.append (row)
		return

###--- globals ---###

cmds = [
	# 0. all markers
	'''select _Marker_key
		from mrk_marker''',

	# 1. count of references for each marker (no longer de-emphasizing
	# curatorial refs, load refs, etc.)
	'''select r._Marker_key, count(r._Refs_key) as numRef
	from mrk_reference r
	group by r._Marker_key''',

	# 2. count of sequences for each marker
	'''select _Marker_key, count(1) as numSeq
		from seq_marker_cache
		group by _Marker_key''',

	# 3. count of alleles for each marker
	'''select m._Marker_key, count(1) as numAll
		from all_marker_assoc m, voc_term t, all_allele a
		where m._Status_key = t._Term_key
			and t.term != 'deleted'
			and m._Allele_key = a._Allele_key
			and a.isWildType != 1
		group by m._Marker_key''',

	# 4. count of antibodies for the marker
	'''select _Marker_key, count(distinct _Antibody_key) as antibodyCount
		from gxd_antibodymarker
		group by _Marker_key''',

	# 5. count of expression assays for each marker
	# (omit Recombinase reporter assays)
	'''select _Marker_key, count(_Assay_key) as numAssay
		from gxd_assay
		where _AssayType_key != 11
		group by _Marker_key''',

	# 6. count of orthologs for each marker
	'''select h1._Marker_key, count(distinct h2._Organism_key) as numOrtho
		from mrk_homology_cache h1,
			mrk_homology_cache h2
		where h1._Class_key = h2._Class_key
			and h1._Organism_key = 1
			and h2._Organism_key != 1
		group by h1._Marker_key''',

	# 7. count of gene trap insertions for each marker (gene trap alleles
	# which have at least one sequence with coordinates)
	'''select ama._Marker_key, count(1) as numGeneTraps
		from all_marker_assoc ama,
			all_allele aa
		where ama._Allele_key = aa._Allele_key
			and aa._Allele_Type_key = 847121
			and exists (select 1
				from seq_allele_assoc saa,
				    seq_coord_cache scc
				where aa._Allele_key = saa._Allele_key
				    and saa._Sequence_key = scc._Sequence_key)
		group by ama._Marker_key''',

	# 8. count of GXD results for each marker
	'''select _Marker_key, count(distinct _Expression_key) as resultCount
		from gxd_expression
		where isForGXD = 1
		group by _Marker_key''',

	# 9. count of GXD Index entries for each marker
	'''select _Marker_key, count(1) as indexCount
		from gxd_index
		group by _Marker_key''',

	# 10. count of tissues associated with each marker
	'''select _Marker_key, count(distinct _Structure_key) as tissueCount
		from gxd_expression
		where isForGXD = 1
		group by _Marker_key''',

	# 11. count of expression image panes associated with each marker
	'''select _Object_key as _Marker_key,
			count(distinct _ImagePane_key) as paneCount
		from img_cache
		where _ImageMGIType_key = 8
			and _MGIType_key = 2
		group by _Marker_key''',

	# 12. count of mapping experiments associated with each marker
	'''select _Marker_key, count(distinct _Expt_key) as mappingCount
		from mld_expt_marker
		group by _Marker_key''',

	# 13. count of RefSeq sequences associated with each marker
	'''select c._Marker_key, count(distinct c._Sequence_key) as numSeq
		from seq_marker_cache c, acc_accession a
		where c._Sequence_key = a._Object_key
			and a.private = 0
			and a.preferred = 1
			and a._MGIType_key = 19
			and a._LogicalDB_key = 27
		group by c._Marker_key''',

	# 14. count of Uniprot sequences associated with each marker
	'''select c._Marker_key, count(distinct c._Sequence_key) as numSeq
		from seq_marker_cache c, acc_accession a
		where c._Sequence_key = a._Object_key
			and a.private = 0
			and a._MGIType_key = 19
			and a._LogicalDB_key in (13, 41)
		group by c._Marker_key''',

	# 15. count of cDNA sources associated with each marker
	'''select m._Marker_key, count(distinct p._Probe_key) as cdnaCount
		from prb_probe p,
			prb_source s,
			voc_term t,
			prb_marker m
		where t.term = 'cDNA'
			and t._Term_key = p._SegmentType_key
			and p._Source_key = s._Source_key
			and s._Organism_key = 1
			and p._Probe_key = m._Probe_key
			and m.relationship in ('E', 'P')
		group by m._Marker_key''',

	# 16. count of microarray probesets associated with each marker
	'''select _Object_key as _Marker_key, count(1) as affyCount
		from acc_accession
		where _MGIType_key = 2
			and _LogicalDB_key in (select _Object_key
				from mgi_setmember sm, mgi_set s
				where sm._Set_key = s._Set_key
					and s.name = 'MA Chip')
		group by _Object_key''',

	# 17. count of phenotype images for each marker
	'''select _Object_key as _Marker_key,
			count(distinct _Image_key) as imageCount
		from img_cache
		where _ImageMGIType_key = 11
			and _MGIType_key = 2
		group by _Marker_key''',

	# 18. count of human diseases for the marker.  omit markers that have
	# no human orthologs.
	'''select m._Marker_key, count(distinct m._Term_key) as diseaseCount
		from mrk_omim_cache m
		where m.qualifier is null
			and m._Organism_key in (1,2)
		group by m._Marker_key''',

	# 19. count of alleles for the marker which are associated with
	# human diseases.  omit markers that have no human orthologs.
	'''select a._Marker_key, count(distinct a._Allele_key) as alleleCount
		from all_allele a,
			gxd_allelegenotype gag,
			voc_annot va,
			voc_term vt
		where a._Allele_key = gag._Allele_key
			and gag._Genotype_key = va._Object_key
			and va._AnnotType_key = 1005
			and a.isWildType = 0
			and va._Qualifier_key = vt._Term_key
			and vt.term is null
			and exists (select 1
				from mrk_homology_cache h1,
					mrk_homology_cache h2
				where a._Marker_key = h1._Marker_key
				and h1._Organism_key = 1
				and h1._Class_key = h2._Class_key
				and h2._Organism_key = 2)
		group by a._Marker_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Marker_key', ReferenceCount, SequenceCount,
	SequenceRefSeqCount, SequenceUniprotCount, AlleleCount,
	GOCount, GxdAssayCount, GxdResultCount, GxdLiteratureCount,
	GxdTissueCount, GxdImageCount, OrthologCount, GeneTrapCount,
	MappingCount, CdnaSourceCount, MicroarrayCount,
	PhenotypeImageCount, HumanDiseaseCount, AllelesWithDiseaseCount,
	AntibodyCount, SnpCount
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_counts'

# global instance of a MarkerCountsGatherer
gatherer = MarkerCountsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
