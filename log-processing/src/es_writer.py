import requests
import json
import logging
import time
from typing import Optional
from datetime import datetime, timezone
from enum import Enum, EnumMeta

# Hard-code a size in bytes of how close we want Bulk API Request payloads to be to
# the maximum supported value
MAX_HTTP_BULK_API_SIZE = int(0.9* 100 * (2**20))  # 90 MB

# Set logging format and level (default is warning)
# All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgo-entity-api.log`
# Log rotation is handled via logrotate on the host system with a configuration file
# Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes
#logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s:%(lineno)d: %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

class ESWriter:
    # Throw in extra classes to get the syntactic sugar an enumeration should have to support the 'in' operator.
    # https://stackoverflow.com/a/10446010/1119928
    # https://stackoverflow.com/a/65225753/1119928
    class MetaEnum(EnumMeta):
        def __contains__(cls, item):
            try:
                cls(item)
            except ValueError:
                return False
            return True

    # An enumeration of the action to be taken when a document with the _id already exists in the index.
    class ActionOnExistingDocType(str, Enum, metaclass=MetaEnum):
        REMOVE_THEN_INDEX = 'remove_then_index' # Delete the document with _id if it exists, then index the
                                                # new/updated JSON as the document.
        UPDATE_INDEX = 'update_index' # Update the document with _id if it exists, so the new/updated JSON
                                      # adds to the index's revisions for the document.
        SKIP_INDEX = 'skip_index' # Leave the existing document in the index, and ignore the new/updated JSON,
                                  # for example, when restarting a failed process which inserted some data.
        
    # An enumeration support 'in' operations, containing the allowed block types for an index.
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-blocks.html
    class IndexBlockType(str, Enum, metaclass=MetaEnum):
        METADATA = 'metadata' # Disable metadata changes, such as closing the index.
        READ = 'read' # Disable read operations.
        READ_ONLY = 'read_only' # Disable write operations and metadata changes.
        WRITE = 'write' # Disable write operations. However, metadata changes are still allowed.
        NONE = 'none' # Locally defined, used to remove other block types in this enumeration.

    # An enumeration support 'in' operations, containing the fill strategy to execute.
    class FillStrategyType(str, Enum, metaclass=MetaEnum):
        EMPTY_FILL = 'empty_fill'   # Empty the active index,
                                    # then write documents to it.
        CREATE_FILL_SWAP = 'create_fill_swap'   # Write documents to a new, offline index,
                                                # then make it active
        CREATE_FILL_HALT = 'create_fill_halt'   # Write documents to a new, offline index,
                                                # then do nothing, leaving it for manual work.
        CLONE_ADD_SWAP = 'clone_add_swap'   # Clone the active index to a new, offline index,
                                            # add documents to it, then make it active
        CLONE_ADD_HALT = 'clone_add_halt'   # Clone the active index to a new, offline index,
                                            # add documents to it, then do nothing, leaving it for manual work.

    # An enumeration support 'in' operations, containing the OpenSearch aggregate queries
    # supported by this class
    class AggQueryType(str, Enum, metaclass=MetaEnum):
        MAX = 'max' # maximum value, or newest timestamp
        MIN = 'min' # minimum value, or oldest timestamp

    def __init__(self, elasticsearch_url:str, timeout:int = 60, max_retries: int = 5, backoff_factor: float = 1.5):
        self.elasticsearch_url = elasticsearch_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    # Get a supported aggregate value of a document field, such as the newest or oldest date of
    # some timestamp in the doc.
    #
    # KBKBKB @TODO see important note on method with same name and different signature
    def get_document_agg_value(self, index_name:str, field_name:str, es_post_search_query_data:str) -> str:

        headers = {'Content-Type': 'application/json'}
        
        try:
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_search?size=0"
                                 , headers=headers
                                 , data=es_post_search_query_data)

            if rspn.ok:
                # KBKBKB This processing isn't appropriate here.  Should be returned to the caller who
                # KBKBKB specified the query.
                rspn_json = json.loads(rspn.text)

                # result = rspn_json['aggregations']['agg_query_result']["value_as_string"]
                
                value_in_milliseconds = rspn_json['aggregations']['agg_query_result']['value']

                if value_in_milliseconds is None:
                    # It is probably perfectly reasonable that aggregation requested couldn't be queried if
                    # the index is empty.  But log as an "error" let the caller decide how to behave.
                    msg = f"Unable to query the aggregate value from" \
                          f" the {field_name} field of the" \
                          f" {index_name} index using" \
                          f" the query: '{es_post_search_query_data}')"
                    logger.error(msg)
                    raise Exception(msg)
                # Convert the 'value_in_milliseconds' timestamp to a UTC string, since ElasticSearch
                # will not provide an accompanying 'value_as_string' if the queried field is
                # a numeric timestamp.
                return datetime.fromtimestamp(value_in_milliseconds/1000.0
                                              , tz=timezone.utc)
            else:
                logger.error(f"Aggregate query failed on index: {index_name}"
                             f" for field_name={field_name}:")
                logger.error(f"Error Message: {rspn.text}")
        except Exception as e:
            msg = f"Exception encountered executing ESWriter.get_document_agg_value()" \
                  f" with index_name='{index_name}'," \
                  f" and field_name={field_name}:"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise e

    # 
    # KBKBKB @TODO - This is a relatively hard-coded aggregate query which used Painless to support the
    # KBKBKB @TODO - DTN directories which became a part of the "File Downloads" process of the
    # KBKBKB @TODO - Log Processing portfolio.  It should be refactored, and the query payload should
    # KBKBKB @TODO - probably be passed in from the process which knows what it needs, rather than
    # KBKBKB @TODO - existing in this helper class for the portfolio.
    #
    # KBKBKB @TODO - In the meantime, reimplement above to do exactly that, at least in support of the
    # KBKBKB @TODO - relatively simple query payload of the UBKG Downloads process.
    # Get a supported aggregate value of a document field, such as the newest or oldest date of
    # some timestamp in the doc.
    def KBKBKBget_document_agg_value(self, index_name, field_name, agg_name_enum: AggQueryType) -> str:
        if agg_name_enum not in self.AggQueryType:
            print(f"agg_name_enum='{agg_name_enum}' is not a supported aggregation.")
            logger.error(f"In ESWriter.get_document_agg_value() with index_name='{index_name}'"
                         f" and field_name='{field_name}',"
                         f" agg_name_enum='{agg_name_enum}' is not a supported aggregation.")
            raise Exception(f"agg_name_enum='{agg_name_enum}' is not a supported aggregation.")

        headers = {'Content-Type': 'application/json'}
        q = '''
            POST file_downloads/_search
            {
              "size": 0,
              "aggs": {
                "by_xfer_node": {
                  "terms": {
                    "script": {
                      "lang": "painless",
                      "source": """
                        if (doc.containsKey('provenance.S3-log-data-to-ES.source_S3_Object.keyword') && !doc['provenance.S3-log-data-to-ES.source_S3_Object.keyword'].empty) {
                          String path = doc['provenance.S3-log-data-to-ES.source_S3_Object.keyword'].value;
                          int first = path.indexOf('/', "logged-info-indexing/".length());
                          if (first == -1) return "";
                          int second = path.indexOf('/', first + 1);
                          if (second == -1) return path.substring(first + 1);
                          return path.substring(first + 1, second);
                        } else {
                          return "";
                        }
                      """
                    }
                  },
                  "aggs": {
                    "by_protocol": {
                      "terms": {"field": "protocol.keyword"},
                      "aggs": {"latest_request": {"max": {"field": "download_date_time"}}}
                    }
                  }
                }
              },
              "query": {
                "bool": {
                  "must": [{"regexp": {"provenance.S3-log-data-to-ES.source_S3_Object.keyword": "logged-info-indexing/file_xfer/.*"}}]
                }
              }
            }
        '''
        
        try:
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_search?size=0"
                                 , headers=headers
                                 , data=q)
            if rspn.ok:
                rspn_json = json.loads(rspn.text)
                result = {bucket["key"]: bucket["latest_request"]["value_as_string"]
                          for bucket in rspn_json["aggregations"]["by_xfer_node"]["buckets"]
                }
                
                value_in_milliseconds = rspn_json['aggregations']['agg_query_result']['value']
                if value_in_milliseconds is None:
                    # It is probably perfectly reasonable that aggregation requested couldn't be queried if
                    # the index is empty.  But log as an "error" let the caller decide how to behave.
                    msg = f"Unable to query the aggregate {agg_name_enum} value from" \
                          f" the {field_name} field of the" \
                          f" {index_name} index using" \
                          f" the query: '{q}')"
                    logger.error(msg)
                    raise Exception(msg)
                # Convert the 'value_in_milliseconds' timestamp to a UTC string, since ElasticSearch
                # will not provide an accompanying 'value_as_string' if the queried field is
                # a numeric timestamp.
                return datetime.fromtimestamp(value_in_milliseconds/1000.0
                                              , tz=timezone.utc)
            else:
                logger.error(f"Aggregate query {agg_name_enum}"
                             f" failed on index: {index_name}"
                             f" for field_name={field_name}:")
                logger.error(f"Error Message: {rspn.text}")
        except Exception as e:
            msg = f"Exception encountered executing ESWriter.get_document_agg_value()" \
                  f" with index_name='{index_name}'," \
                  f" and field_name={field_name}:"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise e

    # This method uses the "Lucene query string syntax" to run a "query parameter search."
    # Per the links below, "Query parameter searches do not support the full Elasticsearch Query DSL but
    # are handy for testing."
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html
    #
    # delete_document(self, index_name, uuid) could become a facade for
    # delete_fieldmatch_document(self, index_name, "uuid", uuid) to transition to Elasticsearch's
    # Query DSL JSON style.
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
    def delete_document_if_exists_by_id(self, index_name, doc_id):
        try:
            # To check if a document exists: HEAD movies / _doc / < doc - id >
            # If the document exists, you get back a 200 OK response, and
            # if it doesn’t, you get back a 404 - Not Found error.
            exists_rspn = requests.head(url=f"{self.elasticsearch_url}/{index_name}/_doc/{doc_id}")
            if exists_rspn.ok:
                rspn = requests.delete(url=f"{self.elasticsearch_url}/{index_name}/_doc/{doc_id}")
                if rspn.ok:
                    logger.info(f"Deleted doc with doc_id: {doc_id} from index: {index_name}")
                else:
                    logger.error(f"Failed to delete doc with doc_id: {doc_id} from index: {index_name}")
                    logger.error(f"Error Message: {rspn.text}")
            else:
                logger.info(f"No document found to delete with doc_id: {doc_id} from index: {index_name}")
        except Exception:
            msg = "Exception encountered during executing ESWriter.delete_document_if_exists_by_id()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def document_exists_in_index(self, index_name='index', doc_id=None):
        try:
            rspn = requests.head(url=f"{self.elasticsearch_url}/{index_name}/_doc/{doc_id}")
            return rspn.status_code in [200, 201, 202]
        except Exception as e:
            msg = "Exception encountered during executing ESWriter.document_exists_in_index()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise e
            
    # KBKBKB @TODO - write_or_update_document() has an inconsistent return contract: it returns
    # KBKBKB @TODO - a success string on success, None on an HTTP-level failure (no exception),
    # KBKBKB @TODO - or an error message string on an exception -- so callers cannot reliably
    # KBKBKB @TODO - distinguish success from failure by type or truthiness alone. Should be
    # KBKBKB @TODO - changed to raise on any failure, matching index_document_or_raise() below,
    # KBKBKB @TODO - with all callers throughout the log-processing portfolio repaired to expect
    # KBKBKB @TODO - the new raise-on-failure behavior.
    def write_or_update_document(self, index_name='index', type_='_doc', doc='', doc_id=''):
        try:
            headers = {'Content-Type': 'application/json'}
            rspn = requests.put(url=f"{self.elasticsearch_url}/{index_name}/{type_}/{doc_id}"
                                ,headers=headers
                                ,data=doc)
            if rspn.status_code in [200, 201, 202]:
                logger.info(f"Added doc using doc_id: {doc_id} to index: {index_name}")
                return f"Added doc using doc_id: {doc_id} to index: {index_name}"
            else:
                logger.error(f"Failed to write doc using doc_id: {doc_id} to index: {index_name}")
                logger.error(f"Error Message: {rspn.text}")
                logger.info("==============ESWriter.write_or_update_document(): request body of JSON source==============")
                logger.info(doc)
        except Exception:
            msg = "Exception encountered during executing ESWriter.write_or_update_document()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            return msg

    # Execute a Query DSL search body against index_name's _search endpoint and return
    # the full parsed JSON response (e.g. 'aggregations' and/or 'hits', depending on the
    # query). Unlike get_document_agg_value() and KBKBKBget_document_agg_value(), this
    # method makes no assumption about the shape of the query or its response -- callers
    # interpret the returned dict themselves.
    #
    # Raises an Exception on a non-2xx HTTP status or a request-level exception, rather
    # than silently returning None on failure.
    def execute_search(self, index_name: str, query_body: str) -> dict:
        headers = {'Content-Type': 'application/json'}
        try:
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_search"
                                 ,headers=headers
                                 ,data=query_body
                                 ,timeout=self.timeout)
        except Exception as e:
            msg = f"Exception encountered executing ESWriter.execute_search()" \
                  f" with index_name='{index_name}': {e}"
            logger.exception(msg)
            raise Exception(msg) from e

        if not rspn.ok:
            msg = f"Search against index: {index_name} failed with" \
                  f" status_code {rspn.status_code}. Error Message: {rspn.text[:500]}"
            logger.error(msg)
            raise Exception(msg)

        return rspn.json()

    # PUT doc as index_name/{type_}/doc_id, replacing any existing document with that
    # _id. Unlike write_or_update_document() above, this method raises an Exception on
    # any failure -- an HTTP error status or a request-level exception -- rather than
    # returning None or an ambiguous message string. Callers that need an all-or-nothing
    # write guarantee (e.g. only proceeding if every one of several writes succeeds)
    # should use this method rather than write_or_update_document().
    def index_document_or_raise(self, index_name: str, doc_id: str, doc: str, type_: str = '_doc'):
        headers = {'Content-Type': 'application/json'}
        try:
            rspn = requests.put(url=f"{self.elasticsearch_url}/{index_name}/{type_}/{doc_id}"
                               ,headers=headers
                               ,data=doc)
        except Exception as e:
            msg = f"Exception encountered executing ESWriter.index_document_or_raise()" \
                  f" with doc_id='{doc_id}', index_name='{index_name}': {e}"
            logger.exception(msg)
            raise Exception(msg) from e

        if rspn.status_code not in [200, 201, 202]:
            msg = f"Failed to write doc using doc_id: {doc_id} to index: {index_name}." \
                  f" Error Message: {rspn.text[:500]}"
            logger.error(msg)
            raise Exception(msg)

        logger.info(f"Added doc using doc_id: {doc_id} to index: {index_name}")
        
    def delete_index(self, index_name):
        try:
            rspn = requests.delete(url=f"{self.elasticsearch_url}/{index_name}")

            if rspn.ok:
                logger.info(f"Deleted index: {index_name}")
            else:
                logger.error(f"Failed to delete index: {index_name} in elasticsearch.")
                logger.error(f"Error Message: {rspn.text}")
        except Exception:
            msg = "Exception encountered executing ESWriter.delete_index()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def create_index_unless_exists(self, index_name):
        exists_rspn = requests.head(url=f"{self.elasticsearch_url}/{index_name}")
        if exists_rspn.ok:
            logger.debug(f"Not creating index_name={index_name} because it already exists.")
            return

        try:
            rspn = requests.put(f"{self.elasticsearch_url}/{index_name}")
            if rspn.ok:
                logger.info(f"Created index: {index_name}")
            else:
                logger.error(f"Failed to create index: {index_name} in elasticsearch.")
                logger.error(f"Error Message: {rspn.text}")
                raise Exception(f"Failed to create index: {index_name} in"
                                f" elasticsearch due to {rspn.text}")
        except Exception as e:
            msg = "Exception encountered executing ESWriter.create_index()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise e

    def verify_exists(self, index_name):
        # KBKBKB @TODO this needs to return a bool based upon the range of the
        # KBKBKB @TODO HTTP Response Code, and all usages need to be refactored,
        # KBKBKB @TODO including appropriateness for fill strategy e.g.
        # KBKBKB @TODO CREATE_FILL_HALL should expect not verify_exists() rather
        # KBKBKB @TODO than verify_exists() for the "fill" index name.
        return requests.head(url=f"{self.elasticsearch_url}/{index_name}")

    def empty_index(self, index_name):
        headers = {'Content-Type': 'application/json'}
        match_all_query = '{ "query": { "match_all": {} } }'
        try:
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_delete_by_query?conflicts=proceed"
                                 ,headers=headers
                                 ,data=match_all_query)
            if rspn.ok:
                logger.info(f"Emptied index: {index_name}")
            else:
                logger.error(f"Failed to empty index: {index_name} in elasticsearch.")
                logger.error(f"Error Message: {rspn.text}")
        except Exception:
            msg = "Exception encountered executing ESWriter.empty_index()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    # Use the dedicated API to set a recognized block on an index.
    # N.B. Such blocks are undone using dynamic index settings rather than with a dedicated API
    # e.g. PUT your_index/_settings {"index": {"blocks.read_only": false}}
    # https://opensearch.org/docs/latest/api-reference/cluster-api/cluster-settings/
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-blocks.html
    def set_index_block(self, index_name:str, block_enum:IndexBlockType):
        if block_enum not in ESWriter.IndexBlockType:
            raise ValueError(f"'{block_enum}' is not a block name supported by ESWriter.IndexBlockType")
        try:
            if block_enum is ESWriter.IndexBlockType.NONE:
                headers = {'Content-Type': 'application/json'}
                payload_json = '{"index": {"blocks.write": false, "blocks.read_only": false,  "blocks.read_only_allow_delete": false}}'
                rspn = requests.put(url=f"{self.elasticsearch_url}/{index_name}/_settings"
                                    ,headers=headers
                                    ,data=payload_json)
            else:
                es_block_name = block_enum.value
                rspn = requests.put(url=f"{self.elasticsearch_url}/{index_name}/_block/{es_block_name}")
        except Exception as e:
            msg = "Exception encountered during executing ESWriter.set_index_block()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise e

        response_dict = json.loads(rspn.text)
        if rspn.status_code in [200, 201, 202] and 'acknowledged' in response_dict and response_dict['acknowledged']:
            # {
            #     "acknowledged": true,
            #     "shards_acknowledged": true,
            #     "indices": [{
            #         "name": "my-index-000001",
            #         "blocked": true
            #     }]
            # }
            logger.info(f"Set '{block_enum}' block on index: {index_name}")
            return
        else:
            logger.error(f"Failed to set '{block_enum}' block on index: {index_name}")
            logger.error(f"Error Message: {rspn.text}")
            raise Exception(f"Failed to set '{block_enum}' block on"
                            f" index: {index_name}, with"
                            f" status_code {rspn.status_code}.  See logs.")

    # Use the Clone API of OpenSearch to clone the source index to the target
    # https://opensearch.org/docs/latest/api-reference/index-apis/clone/
    def clone_index(self, source_index_name, target_index_name):
        # Clone the source index into the target index, and set the target to read/write mode.
        try:
            rspn = requests.put(url=f"{self.elasticsearch_url}/{source_index_name}/_clone/{target_index_name}")
        except Exception as e:
            msg = f"During clone_index('{source_index_name}', '{target_index_name}')," \
                  f" encountered {e.__class__} exception."
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise Exception(f"Failed to clone source index: {source_index_name} "
                            f" to target index: {target_index_name} due to"
                            f" {e.__class__} exception.  See logs.")

        if rspn.ok:
            logger.info(f"Cloned source index: {source_index_name} to target index: {target_index_name}")
        else:
            logger.error(f"Failed to clone source index: {source_index_name} "
                         f" to target index: {target_index_name}.")
            logger.error(f"Error Payload: {rspn.text}")
            response_error_list = json.loads(rspn.text)
            raise Exception(f"Failed to clone source index: {source_index_name} "
                            f" to target index: {target_index_name} with"
                            f" status_code-{response_error_list['status']},"
                            f" reason-{response_error_list['error']['reason']}. See logs.")

    # Wait for the target index to be "green" or the wait_in_seconds to expire. Raise
    # an exception only if the target index is not ready
    # https://opensearch.org/docs/1.2/opensearch/rest-api/cluster-health/
    def wait_until_index_green(self, index_name, wait_in_secs):
        # GET /_cluster/health/target_index?wait_for_status=green&timeout=30s
        remaining_wait_time = wait_in_secs
        SECS_PER_MINUTE = 60
        try:
            while remaining_wait_time > 0:
                # Wait in repeated 1 minute loops, then wait whatever time remains as a balance
                loop_wait_time = min(SECS_PER_MINUTE, remaining_wait_time)
                rspn = requests.get(f"{self.elasticsearch_url}/_cluster/health/{index_name}?"
                                    f"wait_for_status=green&"
                                    f"timeout={loop_wait_time}s")
                if rspn.ok:
                    break
                remaining_wait_time -= loop_wait_time
                logger.info(f"{index_name} not green in past {loop_wait_time} seconds."
                            f" Waiting {remaining_wait_time} more seconds.")
        except Exception as e:
            msg = f"During wait_until_index_green('{index_name}', '{wait_in_secs}')," \
                  f" encountered {e.__class__} exception."
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise Exception(f"Failed during wait for index: {index_name} "
                            f" to reach \"green\" health within {wait_in_secs} seconds due to"
                            f" {e.__class__} exception.  See logs.")

        if rspn.ok:
            logger.info(f"Wait for index: {index_name} to reach \"green\" health complete.")
            return
        else:
            response_error_list = json.loads(rspn.text)
            if response_error_list['timed_out'] or response_error_list['status'] != 'green':

                logger.error(f"Failed to get \"green\" health for index: {index_name} "
                             f" within {wait_in_secs} seconds.")
                logger.error(f"Error Payload: {rspn.text}")
                raise Exception(f"Failed to get \"green\" health for index: {index_name} "
                                f" within {wait_in_secs} seconds. See logs.")
            else:
                # Do not expect to reach here, given statement at
                # https://opensearch.org/docs/1.2/opensearch/rest-api/cluster-health/#example
                logger.error('Unexpectedly got a non-ok response, for reasons not expected in the response body.')
                logger.error(f"Error Payload: {rspn.text}")

    def prepare_bulk_update_payloads(self, process_doc_dict: dict, index_name: str) -> list[str]:
        """
        Convert a dictionary of {doc_id: document_dict} into Elasticsearch Bulk API
        update payloads, chunked so no payload exceeds MAX_HTTP_BULK_API_SIZE.

        Each document is represented as two NDJSON lines:
          1. The update action metadata line
          2. The actual document update line
        """

        payloads = []
        current_chunk = []
        current_size = 0

        for doc_id, doc in process_doc_dict.items():
            # The _id field can be customized depending on your schema
            if not doc_id:
                raise ValueError(f"Document missing identifier key: {doc}")

            # Prepare the two lines required by the Bulk API
            action_line = json.dumps({"update": {"_index": index_name, "_id": doc_id}})
            doc_line = json.dumps({"doc": doc, "doc_as_upsert": True}, separators=(",", ":"))
            entry = f"{action_line}\n{doc_line}\n"

            entry_size = len(entry.encode("utf-8"))

            # Check if adding this entry would exceed our max size
            if current_size + entry_size > MAX_HTTP_BULK_API_SIZE:
                # Commit current chunk and start a new one
                payloads.append("".join(current_chunk))
                current_chunk = []
                current_size = 0

            current_chunk.append(entry)
            current_size += entry_size

        # Add the final chunk
        if current_chunk:
            payloads.append("".join(current_chunk))

        return payloads

    def exec_bulk_payloads(self, bulk_payloads: list[str], index_name: str) -> Optional[list[str]]:
        """
        Send bulk update payloads to Elasticsearch, handling errors, throttling, and transient failures.

        Retries on HTTP 429 and 5xx responses with exponential backoff.
        Returns a list of error messages if any document updates failed.
        """

        logger.info(f"Begin executing {len(bulk_payloads)} Bulk API commands"
                    f" to OpenSearch Service index '{index_name}'"
                    f" at {self.elasticsearch_url}")

        error_msgs = []

        for idx, body in enumerate(bulk_payloads, start=1):
            url = f"{self.elasticsearch_url}/{index_name}/_bulk"
            logger.info(f"Indexing chunk {idx} of {len(bulk_payloads)} ({len(body)} bytes)")

            for attempt in range(1, self.max_retries + 1):
                try:
                    res = requests.post(
                        url,
                        headers={"Content-Type": "application/x-ndjson"},
                        data=body,
                        timeout=self.timeout,
                    )

                    # Retry if rate-limited or transient server error
                    if res.status_code in (429, 500, 502, 503, 504):
                        wait_time = self.backoff_factor ** attempt
                        logger.info(f"Transient error {res.status_code}"
                                    f" on attempt {attempt},"
                                    f" retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue

                    # Hard failure — don't retry
                    if res.status_code != 200:
                        raise Exception(f"Error indexing documents into {index_name}:"
                                        f" {res.status_code}, {res.text[:500]}")

                    # Success — parse the result
                    res_body = res.json().get("items", [])
                    result_values = [item.get("update") for item in res_body if "update" in item]
                    msgs = [
                        f"{item['_id']}: Update - {item.get('error', {}).get('reason')}"
                        for item in result_values
                        if item["status"] not in [200, 201]
                    ]
                    if msgs:
                        error_msgs.extend(msgs)
                    break  # successful post — exit retry loop

                except (requests.ConnectionError, requests.Timeout) as e:
                    # Retry on network or timeout errors
                    wait_time = self.backoff_factor ** attempt
                    logger.info(f"Network error on attempt {attempt}: {e}."
                                f" Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    continue

                except Exception as e:
                    msg = f"Unrecoverable error posting chunk {idx}: {e}"
                    logger.error(msg)
                    error_msgs.append(msg)
                    break

            else:
                # Exhausted retries
                msg = f"Chunk {idx} failed after {self.max_retries} attempts"
                logger.error(msg)
                error_msgs.append(msg)

            # Gentle pacing between chunks
            if idx != len(bulk_payloads):
                time.sleep(1)

        logger.info("Done executing {len(bulk_payloads)} Bulk API commands"
                    f" to OpenSearch Service index '{index_name}'"
                    f" at {self.elasticsearch_url}")

        return error_msgs if error_msgs else None
