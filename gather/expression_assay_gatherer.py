#!./python
# 
# gathers data for the 'expression_assay' table in the front-end database
#
# HISTORY
#
# 08/27/2012    lec
#       - TR11150/scrum-dog TR10273
#       add checks to gxd_expression
#       Assays that are not fully-coded will not be loaded into this gatherer
#

import Gatherer
import logger
import GXDUtils

###--- Classes ---###

class AssayGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the expression_assay table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for assays,
        #       collates results, writes tab-delimited text file

        def collateResults(self):

                # assay notes

                notes = {}              # assay key -> assay note
                (cols, rows) = self.results[0]

                keyCol = Gatherer.columnNumber (cols, '_Assay_key')
                noteCol = Gatherer.columnNumber (cols, 'assayNote')
                
                for row in rows:
                        key = row[keyCol]
                        notes[key] = row[noteCol]

                logger.debug ('Found notes for %d assays' % len(notes))

                # visualization and probe info

                probes = {}             # assay key -> (probe key, probe name)
                prep = {}               # assay key -> probe prep string
                visual = {}             # assay key -> visualization string

                (cols, rows) = self.results[1]
                assayCol = Gatherer.columnNumber (cols, '_Assay_key')
                probeCol = Gatherer.columnNumber (cols, '_Probe_key')
                nameCol = Gatherer.columnNumber (cols, 'probe_name')
                typeCol = Gatherer.columnNumber (cols, 'type')
                senseCol = Gatherer.columnNumber (cols, 'sense')
                labelCol = Gatherer.columnNumber (cols, 'label')
                visualCol = Gatherer.columnNumber (cols, 'visualization')

                toSkip = [ 'Not Applicable', 'Not Specified' ]

                for row in rows:
                        assayKey = row[assayCol]

                        # basic probe data

                        probes[assayKey] = (row[probeCol], row[nameCol])
                        
                        # probe prep data

                        sense = row[senseCol]
                        label = row[labelCol]

                        p = []
                        if sense not in toSkip:
                                p.append(sense)
                        if label not in toSkip:
                                p.append('labelled with ' + label)
                        if row[typeCol] not in toSkip:
                                p.append(row[typeCol])
                        if p:
                                prep[assayKey] = ' '.join(p)

                        # visualization data

                        visualization = row[visualCol]
                        if visualization not in toSkip:
                                visual[assayKey] = visualization

                # antibody / detection system

                (cols, rows) = self.results[2]
                assayCol = Gatherer.columnNumber (cols, '_Assay_key')
                nameCol = Gatherer.columnNumber (cols, 'antibodyName')
                antibodyCol = Gatherer.columnNumber (cols, '_Antibody_key')
                secondaryCol = Gatherer.columnNumber (cols, 'secondary')
                labelCol = Gatherer.columnNumber (cols, 'label')

                antibody = {}           # assay key -> antibody name
                system = {}             # assay key -> detection system string

                for row in rows:
                        key = row[assayCol]

                        if row[nameCol]:
                                antibody[key] = (row[antibodyCol], row[nameCol])

                                secondary = row[secondaryCol]
                                label = row[labelCol]

                                detectionSystem = ''

                                if secondary not in toSkip:
                                        detectionSystem = secondary

                                if label not in toSkip:
                                        if detectionSystem:
                                                detectionSystem = detectionSystem + ' coupled to '
                                        detectionSystem = detectionSystem + label

                                if detectionSystem:
                                        system[key] = detectionSystem 

                # which assays have images?

                hasImage = {}           # assay key -> 0/1

                (cols, rows) = self.results[3]
                assayCol = Gatherer.columnNumber (cols, '_Assay_key')
                hasImageCol = Gatherer.columnNumber (cols, 'hasImage')

                for row in rows:
                        if row[hasImageCol]:
                                hasImage[row[assayCol]] = 1 

                # most of the final results are in the last query; we will
                # just augment these

                self.finalColumns = self.results[-1][0]
                self.finalResults = self.results[-1][1]

                self.convertFinalResultsToList()

                # now fill in the additional data

                cols = self.finalColumns
                keyCol = Gatherer.columnNumber (cols, '_Assay_key')
                typeCol = Gatherer.columnNumber (cols, '_AssayType_key')
                reporterCol = Gatherer.columnNumber(cols, '_ReporterGene_key')
                gelImagePaneKeyCol = Gatherer.columnNumber(cols, 'gel_imagepane_key')

                for row in self.finalResults:
                        key = row[keyCol]

                        probeKey = None
                        probeName = None
                        visualizedWith = None
                        probePrep = None
                        isDirectDetection = 0
                        note = None
                        detectionSystem = None
                        antibodyKey = None
                        antibodyName = None
                        reporter = None
                        image = 0

                        if key in probes:
                                (probeKey, probeName) = probes[key]
                        if key in antibody:
                                (antibodyKey, antibodyName) = antibody[key]
                        if not probeKey and not antibodyKey:
                                isDirectDetection = 1
                        if key in prep:
                                probePrep = prep[key]
                        if key in visual:
                                visualizedWith = visual[key]
                        if key in notes:
                                note = notes[key]
                        if key in system:
                                detectionSystem = system[key]
                        if key in hasImage:
                                image = 1

                        assayType = Gatherer.resolve (row[typeCol], 'gxd_assaytype', '_AssayType_key', 'assayType')
                        if row[reporterCol]:
                                reporter = Gatherer.resolve (row[reporterCol])

                        self.addColumn ('_Probe_key', probeKey, row, cols)
                        self.addColumn ('probe_name', probeName, row, cols)
                        self.addColumn ('probe_preparation', probePrep, row, cols)
                        self.addColumn ('visualized_with', visualizedWith, row, cols)
                        self.addColumn ('is_direct_detection', isDirectDetection, row, cols)
                        self.addColumn ('note', note, row, cols)
                        self.addColumn ('detection_system', detectionSystem, row, cols)
                        self.addColumn ('_Antibody_key', antibodyKey, row, cols)
                        self.addColumn ('antibody', antibodyName, row, cols)
                        self.addColumn ('assay_type', assayType, row, cols)
                        self.addColumn ('assay_type_seq', GXDUtils.getAssayTypeSeq(row[typeCol]), row, cols)
                        self.addColumn ('reporter_gene', reporter, row, cols)
                        self.addColumn ('has_image', image, row, cols)
                        self.addColumn ('gel_imagepane_key', row[gelImagePaneKeyCol], row, cols)
                return

        def postprocessResults(self):
                return

###--- globals ---###

cmds = [
        '''select a._Assay_key, a.assayNote
                from gxd_assaynote a
                where exists (select 1 from gxd_expression e where a._Assay_key = e._Assay_key)
                order by a._Assay_key''',

        '''select a._Assay_key,
                        p._Probe_key,
                        p.name as probe_name,
                        gpp.type,
                        t1.term as sense,
                        t2.term as label,
                        t3.term as visualization
                from gxd_assay a,
                        gxd_probeprep gpp,
                        prb_probe p,
                        voc_term t1,
                        voc_term t2,
                        voc_term t3
                where exists (select 1 from gxd_expression e where a._Assay_key = e._Assay_key)
                and a._ProbePrep_key = gpp._ProbePrep_key
                        and gpp._Probe_key = p._Probe_key
                        and gpp._Sense_key = t1._Term_key
                        and gpp._Label_key = t2._Term_key
                        and gpp._Visualization_key = t3._Term_key
                        ''',

        '''select a._Assay_key,
                        b.antibodyName,
                        b._Antibody_key,
                        t1.term as secondary,
                        t2.term as label
                from gxd_assay a,
                        gxd_antibodyprep p,
                        gxd_antibody b,
                        voc_term t1,
                        voc_term t2
                where exists (select 1 from gxd_expression e where a._Assay_key = e._Assay_key)
                and a._AntibodyPrep_key = p._AntibodyPrep_key
                        and p._Antibody_key = b._Antibody_key
                        and p._Secondary_key = t1._Term_key
                        and p._Label_key = t2._Term_key
                        ''',

        '''select distinct _Assay_key, hasImage
                from gxd_expression''',

        '''select a._Assay_key,
                        a._AssayType_key,
                        a._Refs_key,
                        a._Marker_key,
                        m.symbol,
                        m.name as marker_name,
                        to_char(a.modification_date, 'MM/DD/YYYY') as modification_date,
                        ma.accID as marker_id,
                        aa.accID as assay_id,
                        a._ReporterGene_key,
                        a._imagepane_key gel_imagepane_key
                from gxd_assay a,
                        mrk_marker m,
                        acc_accession ma,
                        acc_accession aa
                where exists (select 1 from gxd_expression e where a._Assay_key = e._Assay_key)
                        and a._Marker_key = m._Marker_key
                        and a._Assay_key = aa._Object_key
                        and aa._MGIType_key = 8
                        and aa._LogicalDB_key = 1
                        and aa.private = 0
                        and aa.preferred = 1
                        and m._Marker_key = ma._Object_key
                        and ma._MGIType_key = 2
                        and ma._LogicalDB_key = 1
                        and ma.private = 0
                        and ma.preferred = 1''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Assay_key', 'assay_type', 'assay_id', '_Probe_key', 'probe_name', 
        '_Antibody_key', 'antibody', 'detection_system', 'is_direct_detection',
        'probe_preparation', 'visualized_with', 'reporter_gene', 
        'note',
        'has_image', 'gel_imagepane_key','_Refs_key', 
        '_Marker_key', 'marker_id', 'symbol', 'marker_name', 'assay_type_seq', 'modification_date',
        ]

# prefix for the filename of the output file
filenamePrefix = 'expression_assay'

# global instance of a AssayGatherer
gatherer = AssayGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
