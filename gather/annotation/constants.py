#
# All constants used by the annotation_gatherer
#
#


MARKER_MGITYPE = 2        # MGI Type for markers
GENOTYPE_MGITYPE = 12        # MGI Type for genotypes
DO_GENOTYPE_TYPE = 1020    # VOC_AnnotType : DO/Genotype
DO_MARKER_TYPE = 1023    # VOC_AnnotType : DO/Marker (Derived)
MP_MARKER_TYPE = 1015    # VOC_AnnotType : MP/Marker (Derived)
GT_ROSA_KEY = 37270        # marker Gt(ROSA)26Sor
DRIVER_NOTE_TYPE = 1034    # MGI_NoteType Driver
NOT_QUALIFIER_KEY = 1614157    # VOC_Term NOT
GOREL_SYNONYM_TYPE = 1034 # GO Property display synonym
GO_EXTENSION_NOTETYPE_KEY = 1045 # GO Annotation Extension (Property) display _notetype_key
GO_ISOFORM_NOTETYPE_KEY = 1046 # GO Isoform (Property) display _notetype_key

BUDDING_YEAST_LDB_KEY = 114        # logical database key for budding yeast
BUDDING_YEAST_NAME = 'budding yeast'    # organism name for budding yeast
FISSION_YEAST_LDB_KEY = 115        # logical database key for fission yeast
FISSION_YEAST_NAME = 'fission yeast'    # organism name for fission yeast
CHEBI_LDB_KEY = 127            # logical database key for ChEBI

GO_ANNOTTYPE = 'GO/Marker'
MP_GENOTYPE_TYPE = 'Mammalian Phenotype/Genotype'

TERM_SORT_TEMP_TABLE = "tmp_term_sort"
VOCAB_SORT_TEMP_TABLE = "tmp_vocab_sort"
ANNOTTYPE_SORT_TEMP_TABLE = "tmp_annottype_sort"
TERM_DAG_SORT_TEMP_TABLE = "tmp_term_dag_sort"
VOCAB_DAG_SORT_TEMP_TABLE = "tmp_vocab_dag_sort"
OBJECT_DAG_SORT_TEMP_TABLE = "tmp_object_dag_sort"
ISOFORM_SORT_TEMP_TABLE = "tmp_isoform_sort"
