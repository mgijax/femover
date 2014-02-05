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
#import MarkerSnpAssociations
import GOFilter
import GenotypeClassifier

###--- Globals ---###

OMIM_GENOTYPE = 1005		# from VOC_AnnotType
OMIM_HUMAN_MARKER = 1006	# from VOC_AnnotType
NOT_QUALIFIER = 1614157		# from VOC_Term
TERM_MGITYPE = 13		# from ACC_MGIType
DRIVER_NOTE = 1034		# from MGI_NoteType
GT_ROSA = 37270			# MRK_Marker for 'Gt(ROSA)26Sor'

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
			(self.results[19], AllelesWithDiseaseCount,
				'alleleCount'),

			# HumanDiseaseCount will be processed separately for
			# mouse markers; this is only for human markers:

			(self.results[20], HumanDiseaseCount, 'diseaseCount'),

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
			d[mrk][SnpCount] = 0
			#TODO: this column can be removed, since it is not used anywhere.
			#d[mrk][SnpCount] = MarkerSnpAssociations.getSnpCount(
			#	mrk)

		# compile the count of human diseases associated with each
		# mouse marker (taking care to skip any a via complex not
		# conditional genotypes) and add it to the counts in 'd'

		cols, rows = self.results[18]

		genoCol = Gatherer.columnNumber (cols, '_Genotype_key')
		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		termCol = Gatherer.columnNumber (cols, '_Term_key')

		# first need to collate disease terms by marker

		diseasesByMarker = {}

		for row in rows:
			genoKey = row[genoCol]
			termKey = row[termCol]
			markerKey = row[markerCol]

			if GenotypeClassifier.getClass(genoKey) == 'cx':
				continue

			if diseasesByMarker.has_key(markerKey):
				diseasesByMarker[markerKey][termKey] = 1
			else:
				diseasesByMarker[markerKey] = { termKey : 1 } 

		# skip adding this, as it was already added above:
		# counts.append (HumanDiseaseCount)

		for mrkKey in diseasesByMarker.keys():
			diseaseCount = len(diseasesByMarker[mrkKey])

			if d.has_key(mrkKey):
				d[mrkKey][HumanDiseaseCount] = diseaseCount
			elif mrkKey != None:
				d[mrkKey] = { HumanDiseaseCount : diseaseCount }

		logger.debug ('Found OMIM diseases for %d markers' % \
			len(diseasesByMarker))

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
	'''select a._Marker_key, count(a._Assay_key) as numAssay
		from gxd_assay a
		where exists (select 1 from gxd_expression e where a._Assay_key = e._Assay_key)
		and a._AssayType_key != 11
		group by a._Marker_key''',

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

	# 8. count of GXD results for each marker (only for structures that
	# map to EMAPS terms)
	'''select e._Marker_key,
			count(distinct e._Expression_key) as resultCount
		from gxd_expression e
		where e.isForGXD = 1
			and exists (select 1
				from acc_accession a,
					mgi_emaps_mapping m
				where e._Structure_key = a._Object_key
					and a._MGIType_key = 38
					and a.accID = m.accID)
		group by e._Marker_key''',

	# 9. count of GXD Index entries for each marker
	'''select _Marker_key, count(1) as indexCount
		from gxd_index
		group by _Marker_key''',

	# 10. count of tissues associated with each marker
	'''select e._Marker_key,
			count(distinct m.emapsID) as tissueCount
		from gxd_expression e, acc_accession a, mgi_emaps_mapping m
		where e.isForGXD = 1
			and e._Structure_key = a._Object_key
			and a._MGIType_key = 38
			and a.accID = m.accID
		group by e._Marker_key''',

	# 11. count of expression image panes associated with each marker
	'''with imagePanes as (select a._Marker_key, a._ImagePane_key
	from img_imagepane p, img_image i, voc_term t, gxd_assay a
	where t.term = 'Expression'
		and t._Term_key = i._ImageClass_key
		and i._Image_key = p._Image_key
		and i.xDim is not null
		and p._ImagePane_key = a._ImagePane_key
		and exists (select 1
			from gxd_expression e,
				acc_accession aa,
				mgi_emaps_mapping m
			where a._Assay_key = e._Assay_key
				and e.isForGXD = 1
				and e._Structure_key = aa._Object_key
				and aa._MGIType_key = 38
				and aa.accID = m.accID)
	union
	select ga._Marker_key, gi._ImagePane_key
	from img_imagepane p, img_image i, voc_term t, 
		gxd_insituresultimage gi, gxd_insituresult gr, gxd_specimen s,
		gxd_assay ga
	where t.term = 'Expression'
		and t._Term_key = i._ImageClass_key
		and i.xDim is not null
		and i._Image_key = p._Image_key
		and p._ImagePane_key = gi._ImagePane_key
		and gi._Result_key = gr._Result_key
		and gr._Specimen_key = s._Specimen_key
		and exists (select 1
			from gxd_expression e
			where ga._Assay_key = e._Assay_key
				and e.isForGXD = 1)
		and exists (select 1
			from gxd_isresultstructure rs,
				acc_accession aa,
				mgi_emaps_mapping m
			where gr._Result_key = rs._Result_key
				and rs._Structure_key = aa._Object_key
				and aa._MGIType_key = 38
				and aa.accID = m.accID)
		and s._Assay_key = ga._Assay_key)
	select _Marker_key, count(distinct _ImagePane_key) as paneCount
	from imagePanes
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

	# 18. pull OMIM annotations up from genotypes to mouse markers.
	#    Exclude paths from markers to genotypes which involve:
	#	a. recombinase alleles (ones with driver notes)
	#	b. wild-type alleles
	#	c. complex, not conditional genotypes
	#	d. complex, not conditional genotypes with transgenes
	#	e. marker Gt(ROSA)
	'''select distinct gag._Marker_key,
		gag._Genotype_key,
		va._Term_key
	from gxd_allelegenotype gag,
		voc_annot va,
		all_allele a
	where gag._Genotype_key = va._Object_key
		and va._AnnotType_key = %d
		and va._Qualifier_key != %d
		and gag._Allele_key = a._Allele_key
		and a.isWildType = 0
		and not exists (select 1 from MGI_Note mn
			where mn._NoteType_key = %d
			and mn._Object_key = gag._Allele_key)
		and gag._Marker_key != %d
	order by gag._Marker_key''' % (
		OMIM_GENOTYPE, NOT_QUALIFIER, DRIVER_NOTE, GT_ROSA),	

	# 19. count of alleles for the marker which are associated with
	# human diseases.
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
		group by a._Marker_key''',

	# 20. OMIM annotations to human markers
	'''select mm._Marker_key, count(q._Term_key) as diseaseCount
	from voc_annot va,
		mrk_marker mm,
		voc_term q
	where va._AnnotType_key = %d
		and va._Object_key = mm._Marker_key
		and va._Qualifier_key = q._Term_key
		and q.term is null
	group by mm._Marker_key''' % OMIM_HUMAN_MARKER,
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
