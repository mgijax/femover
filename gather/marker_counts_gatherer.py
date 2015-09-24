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
import MarkerUtils
import GOFilter
import GenotypeClassifier
import ReferenceUtils
from IMSRData import IMSRDatabase

# The count of GO annotations for a marker should exclude all annotations with
# an ND (No Data) evidence code.
GOFilter.removeAllND()

###--- Globals ---###

OMIM_GENOTYPE = 1005		# from VOC_AnnotType
OMIM_MARKER = 1016		# from VOC_AnnotType
OMIM_HUMAN_MARKER = 1006	# from VOC_AnnotType
NOT_QUALIFIER = 1614157		# from VOC_Term
TERM_MGITYPE = 13		# from ACC_MGIType
DRIVER_NOTE = 1034		# from MGI_NoteType
GT_ROSA = 37270			# MRK_Marker for 'Gt(ROSA)26Sor'

MUTATION_INVOLVES = 1003
EXPRESSES_COMPONENT = 1004

PHENOTYPE_IMAGE = 6481782	# VOC_Term for phenotype image class

ReferenceCount = 'referenceCount'
DiseaseRelevantReferenceCount = 'diseaseRelevantReferenceCount'
GOReferenceCount = 'goReferenceCount'
PhenotypeReferenceCount = 'phenotypeReferenceCount'
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
ImsrCount = 'imsrCount'
MutationInvolvesCount = 'mutationInvolvesCount'
MpAnnotationCount = 'mpAnnotationCount'
MpAlleleCount = 'mpAlleleCount'
StrainCount = 'strainCount'
OtherAnnotCount = 'otherAnnotationCount'

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

		# pre-process the allele counts

		alleleCols = ['_Marker_key', 'numAlleles' ]
		alleleRows = []
		markerCounts = MarkerUtils.getAlleleCounts()

		for markerKey in markerCounts.keys():
			alleleRows.append (
				[ markerKey, markerCounts[markerKey] ] )

		alleleData = [ alleleCols, alleleRows ]

		# pre-process the mutation involves counts

		miCols = [ '_Marker_key', 'numMI' ]
		miRows = []

		miCounts = MarkerUtils.getMutationInvolvesCounts()

		for markerKey in miCounts.keys():
			miRows.append ( [ markerKey, miCounts[markerKey] ] )

		miData = [ miCols, miRows ]

		# list of count types (like field names)
		counts = []

		# initialize dictionary for collecting data per marker
		#	d[marker key] = { count type : count }
		d = {}
		for row in self.results[0][1]:
			d[row[0]] = {}
		logger.debug ('initialized markers data')

		# counts to add in this order, with each tuple being:
		#	(set of results, count constant, count column)

		toAdd = [ (self.results[1], ReferenceCount, 'numRef'),
			(self.results[2], SequenceCount, 'numSeq'),
			(alleleData, AlleleCount, 'numAlleles'),
			(goData, GOCount, 'numGO'),
			(miData, MutationInvolvesCount, 'numMI'),
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

			# query 18 is for mouse markers, query 20 for human

			(self.results[18], HumanDiseaseCount, 'diseaseCount'),
			(self.results[19], AllelesWithDiseaseCount,
				'alleleCount'),
			(self.results[20], HumanDiseaseCount, 'diseaseCount'),
			(self.results[22], MpAnnotationCount, 'annotCount'),
			(self.results[23], MpAlleleCount, 'alleleCount'),
			(self.results[24], GOReferenceCount, 'goRefCount'),
			(self.results[25], PhenotypeReferenceCount,
				'phenoRefCount'),
			(self.results[26], StrainCount, 'strainCount'),
			(self.results[27], OtherAnnotCount, 'otherCount'), 

			# IMSR count processed separately
			]

		for (r, countName, colName) in toAdd:
			logger.debug ('Processing %s, %d rows' % (countName,
				len(r[1])) )

			if countName not in counts:
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
		logger.debug("finished initial count processing")

		# compile the count of IMSR lines/mice associated with each marker and
		# add it to the counts in 'd'

		markerKeys = d.keys()
		markerKeys.sort()

		logger.debug("loading mouse accession IDs for IMSR lookup")
		mouseIds = {}
		for row in self.results[21][1]:
			mouseIds[row[0]] = row[1]
		logger.debug ('loaded mouse marker primary ID lookup')
		

		imsrDB = IMSRDatabase()
		imsrCellLines,imsrStrains,imsrMrkCounts = imsrDB.queryAllCounts()
		imsrCellLines=None # not needed
		imsrStrains=None # not needed
		logger.debug("loaded IMSR counts into memory")

		counts.append (ImsrCount)

		for mrk in markerKeys:
			if mrk in mouseIds:
				mouseId = mouseIds[mrk]
				d[mrk][ImsrCount] = (mouseId in imsrMrkCounts) and imsrMrkCounts[mouseId] or 0

		# include the count of disease-related references for each
		# marker

		mrks = ReferenceUtils.getMarkersWithDiseaseRelevantReferences()

		for m in mrks:
			refs = ReferenceUtils.getDiseaseRelevantReferences(m)
			d[m][DiseaseRelevantReferenceCount] = \
				len(refs)

		counts.append(DiseaseRelevantReferenceCount)

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
		from mrk_marker
	''',

	# 1. count of references for each marker (no longer de-emphasizing
	# curatorial refs, load refs, etc.)
	'''select r._Marker_key, count(r._Refs_key) as numRef
	from mrk_reference r
	group by r._Marker_key''',

	# 2. count of sequences for each marker
	'''select _Marker_key, count(distinct _Sequence_key) as numSeq
		from seq_marker_cache
		group by _Marker_key''',

	# 3. count of alleles for each marker (altered to make a bogus query,
	# since we now count alleles differently).  This keeps us from having
	# to re-number the various result sets below when processing them.
	'''select 1 as _Marker_key, 2 as numAlleles''',

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
	'''
	select m._Marker_key, count(distinct m2._marker_key) as numOrtho
    from MRK_Cluster mc 
			join MRK_ClusterMember mcm on mcm._cluster_key = mc._cluster_key
	    	join MRK_Marker m on m._marker_key = mcm._marker_key 
	    	join MRK_ClusterMember mcm2 on (
	    		mcm2._cluster_key = mcm._cluster_key
	    		and mcm2._clustermember_key != mcm._clustermember_key
	    	)
	    	join MRK_Marker m2 on m2._marker_key = mcm2._marker_key
			join VOC_Term clustertype on clustertype._term_key = mc._clustertype_key
			join VOC_Term source on source._term_key = mc._clustersource_key
    where clustertype.term = 'homology'
            and source.term = 'HomoloGene'
            and m._organism_key = 1
            and m2._organism_key != 1
            group by m._marker_key
	''',

	# 7. count of gene trap insertions for each marker (gene trap alleles
	# which have at least one sequence with coordinates)
	'''select aa._Marker_key, count(1) as numGeneTraps
		from all_allele aa
		where aa._Allele_Type_key = 847121
			and exists (select 1
				from seq_allele_assoc saa,
				    seq_coord_cache scc
				where aa._Allele_key = saa._Allele_key
				    and saa._Sequence_key = scc._Sequence_key)
		group by aa._Marker_key''',

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
	'''with temp_table as (
		select distinct i._Image_key,
			m._Marker_key
		from img_image i,
			img_imagepane p,
			img_imagepane_assoc a,
			all_allele aa,
			mrk_marker m
		where i._Image_key = p._Image_key
			and i._ImageClass_key = %d
			and p._ImagePane_key = a._ImagePane_key
			and a._MGIType_key = 11
			and a._Object_key = aa._Allele_key
			and aa._Marker_key = m._Marker_key
		union
		select distinct i._Image_key,
			r._Object_key_2 as _Marker_key
		from img_image i,
			img_imagepane p,
			img_imagepane_assoc a,
			mgi_relationship r
		where i._Image_key = p._Image_key
			and i._ImageClass_key = %d
			and p._ImagePane_key = a._ImagePane_key
			and a._MGIType_key = 11
			and a._Object_key = r._Object_key_1
			and r._Category_key = %d
		)
		select _Marker_key, count(distinct _Image_key) as imageCount
		from temp_table
		group by _Marker_key''' % (PHENOTYPE_IMAGE, PHENOTYPE_IMAGE,
			EXPRESSES_COMPONENT),

	# 18. get a count of distinct OMIM (disease) annotations which have
	# been associated with mouse markers via a set of rollup rules in
	# the production database.
	# Exclude: annotations with a NOT qualifier
	'''select va._Object_key as _Marker_key,
			count(distinct va._Term_key) as diseaseCount
		from voc_annot va
		where va._AnnotType_key = %d
		and va._Qualifier_key != %d
		group by va._Object_key''' % (OMIM_MARKER, NOT_QUALIFIER),

	# 19. count of alleles for the marker which are associated with
	# human diseases.
	'''with tempTable as (select a._Marker_key, a._Allele_key
		from all_allele a,
			gxd_allelegenotype gag,
			voc_annot va,
			voc_term vt
		where a._Allele_key = gag._Allele_key
			and gag._Genotype_key = va._Object_key
			and va._AnnotType_key = 1005
			and a.isWildType = 0
			and va._Qualifier_key = vt._Term_key
		union
		select r._Object_key_2 as Marker_key, a._Allele_key
		from mgi_relationship r,
			mgi_relationship_category c,
			all_allele a,
			gxd_allelegenotype gag,
			voc_annot va,
			voc_term vt
		where a._Allele_key = gag._Allele_key
			and c.name = 'expresses_component'
			and c._Category_key = r._Category_key
			and a._Allele_key = r._Object_key_1
			and gag._Genotype_key = va._Object_key
			and va._AnnotType_key = 1005
			and a.isWildType = 0
			and va._Qualifier_key = vt._Term_key)
		select _Marker_key, count(distinct _Allele_key) as alleleCount
		from tempTable
		group by 1''',

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
	
	# 21. marker acc ids 
	'''
	select m._Marker_key,
                a.accID, 
                a._LogicalDB_key
        from mrk_marker m,
                acc_accession a
        where m._Marker_key = a._Object_key
                and a._MGIType_key = 2
                and a._LogicalDB_key = 1
                and a.preferred = 1
                and m._Organism_key = 1
	''',

	# 22. MP annotation count
	'''select _Marker_key, count(distinct _DerivedAnnot_key) as annotCount
	from %s
	group by _Marker_key''' % MarkerUtils.getSourceAnnotationTable(),

	# 23. MP allele count
	'''select _Marker_key, count(distinct _Allele_key) as alleleCount
	from %s
	group by _Marker_key''' % MarkerUtils.getSourceGenotypeTable(),

	# 24. GO reference count (except for annotations with an ND evidence
	# code)
	'''select va._Object_key as _Marker_key,
		count(distinct ve._Refs_key) as goRefCount
	from voc_annot va, voc_evidence ve, voc_term et
	where va._AnnotType_key = 1000
		and va._Annot_key = ve._Annot_key
		and ve._EvidenceTerm_key = et._Term_key
		and et.abbreviation != 'ND'
	group by va._Object_key''', 

	# 25. phenotype reference count (including traditional marker/allele
	# pairs, plus relationships via mutation involves and expresses
	# component).  Assumes that no wild-type alleles appear in the data
	# from the relationship table.)
	'''with temp_table as (
		select _Marker_key,
			_Allele_key 
		from all_allele
		where isWildType = 0
		union
		select r._Object_key_2 as _Marker_key,
			r._Object_key_1 as _Allele_key
		from mgi_relationship r
		where r._Category_key in (%d, %d)
		)
	select t._Marker_key,
		count(distinct r._Refs_key) as phenoRefCount
	from temp_table t,
		mgi_reference_assoc r
	where t._Allele_key = r._Object_key
		and r._MGIType_key = 11
	group by t._Marker_key''' % (MUTATION_INVOLVES, EXPRESSES_COMPONENT),

	# 26. background strains represented in the set of annotations rolled
	# up to the marker.
	'''select sg._Marker_key, count(distinct s.strain) as strainCount
	from %s sg, gxd_genotype g, prb_strain s
	where sg._Genotype_key = g._Genotype_key
		and g._Strain_key = s._Strain_key
	group by sg._Marker_key''' % MarkerUtils.getSourceGenotypeTable(),

	# 27. distinct terms for other MP annotations that didn't roll up to
	# the marker
	'''select m._Marker_key, count(distinct a._Term_key) as otherCount
	from %s m, voc_annot a
	where m._Annot_key = a._Annot_key
	group by m._Marker_key''' % MarkerUtils.getOtherAnnotationsTable()
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Marker_key', ReferenceCount, DiseaseRelevantReferenceCount,
	GOReferenceCount, PhenotypeReferenceCount,
	SequenceCount, SequenceRefSeqCount, SequenceUniprotCount, AlleleCount,
	GOCount, GxdAssayCount, GxdResultCount, GxdLiteratureCount,
	GxdTissueCount, GxdImageCount, OrthologCount, GeneTrapCount,
	MappingCount, CdnaSourceCount, MicroarrayCount,
	PhenotypeImageCount, HumanDiseaseCount, AllelesWithDiseaseCount,
	AntibodyCount, ImsrCount, MutationInvolvesCount, MpAnnotationCount,
	MpAlleleCount, StrainCount, OtherAnnotCount,
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
