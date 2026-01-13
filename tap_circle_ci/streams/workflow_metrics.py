"""tap-circle-ci product-reviews stream module."""

from typing import Dict, List, Tuple

from singer import (
    Transformer,
    clear_bookmark,
    get_bookmark,
    get_logger,
    metrics,
    utils,
    write_record,
    write_state,
)

from .abstracts import IncrementalStream
from .workflows import Workflows
from singer.utils import strftime, strptime_to_utc

LOGGER = get_logger()


class WorkflowMetrics(IncrementalStream):
    """class for jobs stream."""

    stream = "workflow_metrics"
    tap_stream_id = "workflow_metrics"
    key_properties = ["id"]
    url_endpoint = "https://circleci.com/api/v2/insights/PROJECT/workflows/WORKFLOW_NAME?all-branches=true"
    project = None
    replication_key = "created_at"
    valid_replication_keys = ["created_at"]
    config_start_key = "start_date"

    def get_workflows(self, state: Dict) -> Tuple[List, int]:
        """Returns index for sync resuming on interuption."""
        shared_workflow_names = Workflows(self.client).prefetch_workflow_names(self.project)
        last_synced = get_bookmark(state, self.tap_stream_id, "currently_syncing", False)
        last_sync_index = 0
        if last_synced:
            for pos, (workflow_name, _) in enumerate(shared_workflow_names):
                if workflow_name == last_synced:
                    LOGGER.warning("Last Sync was interrupted after product *****%s", workflow_name)
                    last_sync_index = pos
                    break
        LOGGER.info("last index for workflow-jobs %s", last_sync_index)
        return shared_workflow_names, last_sync_index

    def get_records(self, workflow_name: str, bookmark_date: str) -> List:
        # pylint: disable=W0221
        """performs api querying and pagination of response."""
        params = {}
        extraction_url = self.url_endpoint.replace("WORKFLOW_NAME", workflow_name).replace("PROJECT", self.project)
        config_start = self.client.config.get(self.config_start_key, False)
        bookmark_date = bookmark_date or config_start
        bookmark_date = current_max = max(strptime_to_utc(bookmark_date), strptime_to_utc(config_start))
        records = []
        while True:
            response = self.client.get(extraction_url, params, {})
            raw_records = response.get("items", [])
            next_page_token = response.get("next_page_token", None)
            if not raw_records:
                break
            for record in raw_records:
                record_timestamp = strptime_to_utc(record[self.replication_key])
                if record_timestamp >= bookmark_date:
                    current_max = max(current_max, record_timestamp)
                    records.append(record)
                else:
                    next_page_token = None
                    break
            if next_page_token is None:
                break
            params["page-token"] = next_page_token

        return records, current_max

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Sync implementation for `jobs` stream."""
        # pylint: disable=R0914
        with metrics.Timer(self.tap_stream_id, None):
            pipelines, start_index = self.get_workflows(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            prod_len = len(pipelines)
            with metrics.Counter(self.tap_stream_id) as counter:
                for index, (workflow_name, pipeline_id) in enumerate(set(pipelines[start_index:]), max(start_index, 1)):
                    LOGGER.info("Syncing jobs for workflow *****%s (%s/%s)", workflow_name, index, prod_len)
                    bookmark_date = self.get_bookmark(state, workflow_name)
                    records, max_bookmark = self.get_records(workflow_name, bookmark_date)
                    for rec in records:
                        rec["_workflow_name"], rec["_pipeline_id"] = workflow_name, pipeline_id
                        rec["inserted_at"] = utils.now().isoformat()
                        write_record(self.tap_stream_id, transformer.transform(rec, schema, stream_metadata))
                        counter.increment()
                    state = self.write_bookmark(state, workflow_name, strftime(max_bookmark))
                    state = self.write_bookmark(state, "currently_syncing", workflow_name)
                    write_state(state)
            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")
        return state
