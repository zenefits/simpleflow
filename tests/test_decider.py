#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import swf.models

from simpleflow import (
    activity,
    Workflow,
    futures,
)

from swf.models.history import builder
from simpleflow.swf.executor import Executor

DOMAIN = swf.models.Domain('TestDomain')
DEFAULT_VERSION = 'test'


@activity.with_attributes(version=DEFAULT_VERSION)
def increment(x):
    return x + 1


@activity.with_attributes(version=DEFAULT_VERSION)
def double(x):
    return x * 2


class TestWorkflow(Workflow):
    name = 'test_workflow'
    version = 'test_version'
    task_list = 'test_task_list'
    decision_tasks_timeout = '300'
    execution_timeout = '3600'
    tag_list = None      # FIXME should be optional
    child_policy = None  # FIXME should be optional


class TestDefinition(TestWorkflow):
    """
    Executes two tasks. The second depends on the first.

    """
    def run(self):
        a = self.submit(increment, 1)
        assert isinstance(a, futures.Future)

        b = self.submit(double, a)

        return b.result

class TestCallBacks(TestWorkflow):
    """
    Executes two tasks. The second depends on the first.

    """

    on_start_called = False
    on_start_args = None
    on_start_kwargs = None

    on_complete_called = False
    on_complete_args = None
    on_complete_kwargs = None
    on_complete_result = None
    on_complete_history = None

    def run(self, arg1, kwarg1 = 'foo'):
        a = self.submit(increment, 1)
        return a.result

    def on_start(self, args=None, kwargs=None):
        if self.on_start_called:
            assert False

        self.on_start_called = True
        self.on_start_args = args
        self.on_start_kwargs = kwargs

    def on_complete(self, result, history, args, kwargs):
        if self.on_complete_called:
            assert False

        self.on_complete_called = True
        self.on_complete_args = args
        self.on_complete_kwargs = kwargs
        self.on_complete_result = result
        self.on_complete_history = history

def test_workflow_start_complete_callback():
    workflow = TestCallBacks

    executor = Executor(DOMAIN, workflow)

    args = [4]
    kwargs = { 'kwarg1': 'bar' }

    result = 5
    history = builder.History(workflow, input={'args': args, 'kwargs': kwargs})

    decisions, _ = executor.replay(history)

    assert executor._workflow.on_start_called
    assert executor._workflow.on_start_args == args
    assert executor._workflow.on_start_kwargs == kwargs

    executor._workflow.on_start_called = False

    decision_id = history.last_id
    scheduled_id = decision_id + 1

    (history
        .add_activity_task(increment,
                           decision_id=decision_id,
                           last_state='completed',
                           activity_id='activity-tests.test_decider.increment-1',
                           input={'args': args[0]},
                           result=result)
        .add_decision_task_scheduled()
        .add_decision_task_started())

    decisions, _ = executor.replay(history)

    # on_start should only be called once
    assert not executor._workflow.on_start_called

    assert executor._workflow.on_complete_called
    assert executor._workflow.on_complete_args == args
    assert executor._workflow.on_complete_kwargs == kwargs
    assert executor._workflow.on_complete_result == result
    assert executor._workflow.on_complete_history._history == history