"""tap-circle-ci sync."""

from typing import Dict

import singer

from tap_circle_ci.client import Client
from tap_circle_ci.streams import STREAMS

LOGGER = singer.get_logger()


def get_all_projects(client: Client) -> list:
    """Fetches all projects available in the CircleCI account."""
    all_projects = []
    params = {}
    response = client.get(
        endpoint="https://circleci.com/api/v1.1/projects",
        params=params,
        headers={},
    )
    for project in response:
        project_id: str = project["vcs_url"].split("/")[-1]
        response = client.get(
            endpoint=f"https://circleci.com/api/v2/project/{project_id}",
            params={},
            headers={},
        )
        project_slug: str = response.get("slug")
        if not project_slug:
            LOGGER.warning("Could not fetch slug for project ID: %s", project_id)
            continue
        all_projects.append(project_slug)
    return all_projects


def sync(config: dict, state: Dict, catalog: singer.Catalog):
    """performs sync for selected streams."""
    client = Client(config)
    projects = (
        list(filter(None, client.config["project_slugs"].split(" ")))
        if config.get("project_slugs")
        else get_all_projects(client)
    )
    with singer.Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            tap_stream_id = stream.tap_stream_id
            stream_schema = stream.schema.to_dict()
            stream_metadata = singer.metadata.to_map(stream.metadata)
            stream_obj = STREAMS[tap_stream_id](client)
            LOGGER.info("Starting sync for stream: %s", tap_stream_id)
            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)
            singer.write_schema(tap_stream_id, stream_schema, stream_obj.key_properties, stream.replication_key)
            for project in projects:
                stream_obj.project = project
                LOGGER.info("Starting sync for project: %s", project)
                state = stream_obj.sync(
                    state=state, schema=stream_schema, stream_metadata=stream_metadata, transformer=transformer
                )
            singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
