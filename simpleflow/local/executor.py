import logging

from simpleflow import (
    exceptions,
    executor,
    futures,
)
from ..task import ActivityTask, WorkflowTask
from simpleflow.activity import Activity
from simpleflow.workflow import Workflow


logger = logging.getLogger(__name__)


class Executor(executor.Executor):
    """
    Executes all tasks synchronously in a single local process.

    """
    def submit(self, func, *args, **kwargs):
        logger.info('executing task {}(args={}, kwargs={})'.format(
            func, args, kwargs))

        future = futures.Future()

        if isinstance(func, Activity):
            task = ActivityTask(func, *args, **kwargs)
        elif issubclass(func, Workflow):
            task = WorkflowTask(func, *args, __executor=self, **kwargs)

        try:
            future._result = task.execute()
        except Exception as err:
            future._exception = err
            logger.info('rescuing exception: {}'.format(err))
            if isinstance(func, Activity) and func.raises_on_failure:
                raise exceptions.TaskFailed(func.name, err.message)
        finally:
            future._state = futures.FINISHED

        return future

    def run(self, input=None):
        if input is None:
            input = {}
        args = input.get('args', ())
        kwargs = input.get('kwargs', {})

        return self.run_workflow(*args, **kwargs)
