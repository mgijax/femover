#
# Maps from production-style annotation keys (VOC_Annot._Annot_key) to front-end-style 
# annotation keys (annotation.annotation_key).  Used by annotation_gatherer to collect
# mappings from prod-to-fe while generating annotation table, then to populate new
# annotation_source table using the mapped values.
#

import logger

class AnnotationMapper:
    """
    maps from production-style annotation keys (VOC_Annot._Annot_key) to fe-style ones
    """

    def __init__ (self):
        self.prodToFe = {}
        return
    
    def map(self, prodAnnotKey, feAnnotKey):
        if prodAnnotKey not in self.prodToFe:
            self.prodToFe[prodAnnotKey] = [ feAnnotKey ]
        elif feAnnotKey not in self.prodToFe[prodAnnotKey]:
            self.prodToFe[prodAnnotKey].append(feAnnotKey)
        return
    
    def getFeAnnotKeys(self, prodAnnotKey):
        if prodAnnotKey in self.prodToFe:
            if len(self.prodToFe[prodAnnotKey]) > 1:
                self.prodToFe[prodAnnotKey].sort()
            return self.prodToFe[prodAnnotKey]
        return []

    def size(self):
        return len(self.prodToFe)