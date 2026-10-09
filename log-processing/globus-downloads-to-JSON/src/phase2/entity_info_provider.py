"""
EntityInfoProvider -- provides dataset info lookups (dataset_type,
application_id) for the augment-with-entity-info phase (Phase 2) of the
globus-downloads-to-JSON pipeline. Used by augment_with_entity_info.py.

Structured as a class specifically because the data source is expected to
change over time (Neo4j directly for now, a real entity-api call later)
without the calling code needing to change -- get_dataset_info_crosswalk()
is meant to be one of potentially several same-shaped methods, all
returning {uuid: {field_name: value, ...}}.
"""

from log_extract_xfer_utils import LogExtractXferUtils

# Returns entity_type, application_id (e.g. hubmap_id), and dataset_type,
# since they are resolved from the same Neo4j row per uuid.  These fields
# also share the same PENDING/UNRESOLVED/UNTRACKED resolution states.
#
# Broadened to also match Upload and Publication nodes in addition to
# Dataset nodes, using Neo4j's label-OR syntax:
# (d:Dataset|Upload|Publication).
#
# dataset_type is Dataset-only, and Neo4j returns null (not an error) for
# that field on Upload/Publication rows, which resolve_entity_info_field()
# treats correctly as a null value.
DATASET_INFO_QUERY = (
    "MATCH (d:Dataset|Upload|Publication) "
    "RETURN DISTINCT d.uuid AS uuid, d.entity_type AS entity_type, "
    "d.dataset_type AS dataset_type, d.hubmap_id AS application_id"
)
DISTINCT_DATASET_TYPES_QUERY = (
    "MATCH (d:Dataset) RETURN DISTINCT d.dataset_type AS dataset_type"
)


class EntityInfoProvider:
    def __init__(self, portfolio_utils: LogExtractXferUtils):
        self.portfolio_utils = portfolio_utils

    def get_dataset_info_crosswalk(self) -> dict:
        """
        Returns {uuid: {'entity_type': ..., 'dataset_type': ...,
        'application_id': ...}}, queried fresh from Neo4j every call via
        LogExtractXferUtils.query_neo4j().
        """
        records = self.portfolio_utils.query_neo4j(DATASET_INFO_QUERY)
        return {
            record["uuid"]: {
                "entity_type": record["entity_type"],
                "dataset_type": record["dataset_type"],
                "application_id": record["application_id"],
            }
            for record in records
        }

    def get_known_dataset_types(self) -> set:
        """
        Returns the set of distinct dataset_type values currently in Neo4j.
        This means the values checked are from the same data source as the
        being checked, neutering the check.  But could later fall
        to a configured list without callers needing to change.

        Only meaningfully applies to Datasets with a dataset_type set.
        """
        records = self.portfolio_utils.query_neo4j(DISTINCT_DATASET_TYPES_QUERY)
        return {record["dataset_type"] for record in records}
