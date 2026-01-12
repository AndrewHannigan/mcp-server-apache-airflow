from typing import Any, Callable, Dict, List, Optional, Union

import mcp.types as types
from airflow_client.client.api.event_log_api import EventLogApi

from src.airflow.airflow_client import api_client

event_log_api = EventLogApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (get_event_logs, "get_event_logs", "List log entries from event log", True),
        (get_event_log, "get_event_log", "Get a specific log entry by ID", True),
    ]


async def get_event_logs(
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_by: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """List log entries from event log.

    Note: Only limit, offset, and order_by are supported in Airflow 2.7.3.
    Other filtering parameters (dag_id, task_id, etc.) were added in later versions.
    """
    kwargs: Dict[str, Any] = {}
    if limit is not None:
        kwargs["limit"] = limit
    if offset is not None:
        kwargs["offset"] = offset
    if order_by is not None:
        kwargs["order_by"] = order_by

    response = event_log_api.get_event_logs(**kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def get_event_log(
    event_log_id: int,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    response = event_log_api.get_event_log(event_log_id=event_log_id)
    return [types.TextContent(type="text", text=str(response.to_dict()))]
