#!./python
# 
# gathers data for the 'referenceCounts' table in the front-end database

# NOTE: To add more counts:
#       1. add a fieldname for the count as a global (like MarkerCount)
#       2. add a new query to 'cmds' in the main program
#       3. add processing for the new query to collateResults(), to tie the
#               query results to the new fieldname in each reference's
#               dictionary
#       4. add the new fieldname to fieldOrder in the main program

import Gatherer
import logger
import GOFilter
import StrainUtils

###--- Globals ---###

MarkerCount = 'markerCount'
ProbeCount = 'probeCount'
MappingCount = 'mappingCount'
GxdIndexCount = 'gxdIndexCount'
GxdResultCount = 'gxdResultCount'
GxdStructureCount = 'gxdStructureCount'
GxdAssayCount = 'gxdAssayCount'
AlleleCount = 'alleleCount'
SequenceCount = 'sequenceCount'
GoAnnotCount = 'goAnnotationCount'
StrainCount = 'strainCount'

error = 'referenceCountsGatherer.error'

###--- Classes ---###

class ReferenceCountsGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the referenceCounts table
        # Has: queries to execute against source db
        # Does: queries for primary data for reference counts,
        #       collates results, writes tab-delimited text file

        def collateResults (self):
                # Purpose: to combine the results of the various queries into
                #       one single list of final results, with one row per
                #       reference

                # list of count types (like field names)
                counts = []

                # initialize dictionary for collecting data per reference
                #       d[reference key] = { count type : count }
                d = {}
                for row in self.results[0][1]:
                        d[row[0]] = {}

                # counts to add in this order, with each tuple being:
                #       (set of results, count constant, count column)

                toAdd = [ (self.results[1], MarkerCount, 'numMarkers'),
                        (self.results[2], ProbeCount, 'numProbes'),
                        (self.results[3], MappingCount, 'numExperiments'),
                        (self.results[4], GxdIndexCount, 'numIndex'),
                        (self.results[5], GxdResultCount, 'numResults'),
                        (self.results[6], GxdStructureCount, 'numStructures'),
                        (self.results[7], GxdAssayCount, 'numAssays'),
                        (self.results[8], AlleleCount, 'numAlleles'),
                        (self.results[9], SequenceCount, 'numSequences'),
                        (self.results[10], StrainCount, 'numStrains'),
                        ]
                for (r, countName, colName) in toAdd:
                        logger.debug ('Processing %s, %d rows' % (countName,
                                len(r[1])) )
                        counts.append (countName)
                        refKeyCol = Gatherer.columnNumber (r[0], '_Refs_key')
                        countCol = Gatherer.columnNumber (r[0], colName)

                        for row in r[1]:
                                refKey = row[refKeyCol]
                                if refKey in d:
                                        d[row[refKeyCol]][countName] = \
                                                row[countCol]
                                else:
                                        raise error('Unknown reference key: %d' % refKey)

                # add in GO annotation counts

                for refKey in GOFilter.getReferenceKeys():
                        if refKey in d:
                                d[refKey][GoAnnotCount] = \
                                    GOFilter.getAnnotationCountForRef(refKey)

                counts.append(GoAnnotCount)

                # compile the list of collated counts in self.finalResults
                # and the list of columns in self.finalColumns

                self.finalResults = []
                referenceKeys = list(d.keys())
                referenceKeys.sort()

                self.finalColumns = [ '_Refs_key' ] + counts

                for referenceKey in referenceKeys:
                        row = [ referenceKey ]
                        for count in counts:
                                if count in d[referenceKey]:
                                        row.append (d[referenceKey][count])
                                else:
                                        row.append (0)

                        self.finalResults.append (row)
                return

###--- globals ---###

cmds = [
        # 0. all references
        'select m._Refs_key from bib_refs m',

        # 1. count of markers for each reference, including refs for alleles and considering
        # "mutation involves" and "expressed component" relationships.  Built using query
        # from marker_to_reference_gatherer.py to help results match these counts.
        '''with marker_alleles as (
                        select _Marker_key, _Allele_key 
                        from all_allele
                        where isWildType = 0
                        union
                        select r._Object_key_2 as _Marker_key, r._Object_key_1 as _Allele_key
                        from mgi_relationship r
                        where r._Category_key in (1003,1004)
                        ),
                marker_refs as (
                        select r._Marker_key, r._Refs_key
                        from mrk_reference r
                        union
                        select t._Marker_key, r._Refs_key
                        from marker_alleles t, mgi_reference_assoc r
                        where t._Allele_key = r._Object_key
                        and r._MGIType_key = 11)
                select r._Refs_key, count(distinct m._Marker_key) as numMarkers
                from marker_refs m, bib_refs r, bib_citation_cache c
                where m._Refs_key = r._Refs_key
                        and m._Refs_key = c._Refs_key
                        and m._Marker_key is not null
                group by 1''',

        # 2. count of probes
        '''select _Refs_key, count(1) as numProbes
                from prb_reference
                group by _Refs_key''',

        # 3. count of mapping experiments
        '''select _Refs_key, count(1) as numExperiments
                from mld_expts
                group by _Refs_key''',

        # 4. count of GXD literature index entries
        '''select _Refs_key, count(1) as numIndex
                from gxd_index
                group by _Refs_key''',

        # 5. count of expression results
        '''select a._Refs_key, count(distinct e._Expression_key) as numResults
                from gxd_assay a, gxd_expression e
                where a._Assay_key = e._Assay_key
                        and e.isForGXD = 1
                group by a._Refs_key''',

        # 6. count of structures with expression results
        '''select _Refs_key, count(distinct vte._term_key) as numStructures
                from gxd_expression e
                join voc_term_emaps vte on
                        vte._emapa_term_key = e._emapa_term_key
                        and vte._stage_key = e._stage_key
                where isForGXD = 1
                group by _Refs_key''',

        # 7. count of expression assays
        '''select a._Refs_key, count(distinct a._Assay_key) as numAssays
                from gxd_assay a, gxd_expression e
                where a._Assay_key = e._Assay_key
                        and e.isForGXD = 1
                group by a._Refs_key''',

        # 8. count of alleles
        '''select r._Refs_key as _Refs_key,
                        count(distinct a._Allele_key) as numAlleles
                from mgi_reference_assoc r, all_allele a
                where r._Object_key = a._Allele_key
                        and r._MGIType_key = 11
                        and a.isWildType != 1
                group by r._Refs_key''',

        # 9. count of sequences
        '''select _Refs_key as _Refs_key,
                        count(distinct _Object_key) as numSequences
                from mgi_reference_assoc
                where _MGIType_key = 19
                group by _Refs_key''',
                
        # 10. count of mouse strains per reference
        '''select _Refs_key, count(distinct _Strain_key) as numStrains
                from %s
                group by _Refs_key''' % StrainUtils.getStrainReferenceTempTable(),
        ]

# order of fields (from the query results) to be written to the output file
fieldOrder = [ '_Refs_key', MarkerCount, ProbeCount, MappingCount,
        GxdIndexCount, GxdResultCount, GxdStructureCount,
        GxdAssayCount, AlleleCount, SequenceCount, GoAnnotCount, StrainCount, ]

# prefix for the filename of the output file
filenamePrefix = 'reference_counts'

# global instance of a ReferenceCountsGatherer
gatherer = ReferenceCountsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
