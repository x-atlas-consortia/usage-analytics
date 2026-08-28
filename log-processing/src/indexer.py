import logging

from es_writer import ESWriter

# logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.INFO,
#                     datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

class Indexer:
    def __init__(self, es_url):
        self.elasticsearch_url = es_url
        self.eswriter = ESWriter(self.elasticsearch_url)

    def index(self, doc_id, document, index_name, existing_doc_action=ESWriter.ActionOnExistingDocType.REMOVE_THEN_INDEX):

        if existing_doc_action in [ESWriter.ActionOnExistingDocType.SKIP_INDEX] and \
           self.eswriter.document_exists_in_index(index_name=index_name
                                                  , doc_id=doc_id):
            logger.info(f"Skip updating document with _id: {doc_id} which already exists in index: {index_name}")
            return

        if existing_doc_action in [ESWriter.ActionOnExistingDocType.REMOVE_THEN_INDEX]:
            logger.info(f"Deleting old document with _id: {doc_id} from index: {index_name}")
            self.delete_document_if_exists_by_id(index_name=index_name
                                                 ,doc_id=doc_id)

        logger.info(f"Creating document with _id: {doc_id} at index: {index_name}")
        return self.eswriter.write_or_update_document(index_name=index_name, doc=document, doc_id=doc_id)

    def delete_document_if_exists_by_id(self, index_name, doc_id):
        self.eswriter.delete_document_if_exists_by_id(index_name=index_name
                                                      ,doc_id=doc_id)

    def delete_index(self, index_name):
        self.eswriter.delete_index(index_name)

    def remove_empty_elements(self, d):
        """recursively remove empty lists, empty dicts, or None elements from a dictionary"""

        def empty(x):
            return x is None or x == {} or x == [] or x == ""

        if not isinstance(d, (dict, list)):
            return d
        elif isinstance(d, list):
            return [v for v in (self.remove_empty_elements(v) for v in d) if not empty(v)]
        else:
            return {k: v for k, v in ((k, self.remove_empty_elements(v)) for k, v in d.items()) if not empty(v)}
