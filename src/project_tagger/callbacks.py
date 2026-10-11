"""Callbacks used by Deadline Alerts (the Airflow 3 replacement for SLAs).

A deadline callback is referenced by dotted path, so this module has to be
importable by whichever process runs it: the executor for `SyncCallback`, the
triggerer for `AsyncCallback`. In this project both get `/opt/airflow/src` on
PYTHONPATH.

In production you would point the callback at a Notifier instead, e.g.
`AsyncCallback(SlackWebhookNotifier, kwargs={"text": "..."})`.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


def deadline_missed(**kwargs: Any) -> None:
    """Fires when a Dag run passes its deadline without finishing."""
    log.warning("DEADLINE MISSED: %s", kwargs)
    print(f"DEADLINE MISSED: {kwargs}")


async def deadline_missed_async(**kwargs: Any) -> None:
    """Async variant, for use with `AsyncCallback` (runs in the triggerer)."""
    log.warning("DEADLINE MISSED (async): %s", kwargs)
