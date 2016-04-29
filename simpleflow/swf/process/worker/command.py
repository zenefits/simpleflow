from __future__ import absolute_import

import swf.models

from simpleflow.swf.process.decider import helpers
from .base import (
    Worker,
    ActivityPoller,
)


def make_worker_poller(workflow, domain, task_list, heartbeat, soft_cancel_wait_period, max_restart_count, max_RSS_restart, socket_timeout):
    domain = swf.models.Domain(domain)
    return ActivityPoller(
        domain,
        task_list,
        helpers.load_workflow(domain, workflow),
        heartbeat,
        soft_cancel_wait_period,
        max_restart_count=max_restart_count,
        max_RSS_restart=max_RSS_restart,
        socket_timeout=socket_timeout
    )


def start(workflow, domain, task_list, nb_processes=None, heartbeat=60):
    poller = make_worker_poller(workflow, domain, task_list, heartbeat)
    worker = Worker(poller, nb_processes)
    worker.is_alive = True
    worker.start()
