from __future__ import absolute_import
from .activity import Activity
from . import canvas
from . import task

import inspect


class Workflow(object):
    """
    Main interface to define a workflow by submitting tasks for asynchronous
    execution.

    The actual behavior depends on the executor backend.

    """

    # default workflow name
    name = 'workflow'

    # default workflow version
    version = '1.0'

    # None means the workflow can run on any given task list
    # if set, the workflow can only be process from the given task list
    task_list = None

    # how long should the workflow delay starting (in seconds)
    delayed_start_timer = 0

    # if this is a daemon workflow - that is, is the workflow going to re-schedule itself at the end of execution
    is_daemon = False

    # default decision timeout
    decision_tasks_timeout = 300

    # default execution timeout
    execution_timeout = 300

    def __init__(self, executor):
        self._executor = executor

    def submit(self, activity, *args, **kwargs):
        """
        Submit a function for asynchronous execution.

        :param activity: callable registered as an task.
        :type  activity: activity.Activity | task.ActivityTask | task.WorkflowTask | canvas.Group | canvas.Chain | workflow.Workflow
        :param *args: arguments passed to the task.
        :type  *args: Sequence.
        :param **kwargs: keyword-arguments passed to the task.
        :type  **kwargs: Mapping (dict).

        :returns:
            :rtype: Future.

        """
        # If the activity is a child workflow, call directly
        # the executor
        if inspect.isclass(activity) and issubclass(activity, Workflow):
            return self._executor.submit(activity, *args, **kwargs)
        elif isinstance(activity, (task.ActivityTask, task.WorkflowTask)):
            return self._executor.submit(activity.activity, *activity.args, **activity.kwargs)
        elif isinstance(activity, Activity):
            return self._executor.submit(activity, *args, **kwargs)
        elif isinstance(activity, canvas.Group):
            return activity.submit(self._executor)
        else:
            raise TypeError('Bad type for {} activity ({})'.format(
                activity,
                type(activity)
            ))

    def map(self, activity, iterable):
        """
        Submit a function for asynchronous execution for each value of
        *iterable*.

        :param activity: callable registered as an task.
        :type  activity: task.ActivityTask | task.WorkflowTask.
        :param iterable: collections of arguments passed to the task.
        :type  iterable: Iterable.

        """
        group = canvas.Group(*[task.ActivityTask(activity, i) for i in iterable])
        return self.submit(group).futures

    def starmap(self, activity, iterable):
        """
        Submit a function for asynchronous execution for each value of
        *iterable*.

        :param activity: callable registered as an task.
        :type  activity: task.ActivityTask | task.WorkflowTask.
        :param iterable: collections of multiple-arguments passed to the task
                         as positional arguments. They are destructured using
                         the ``*`` operator.
        :type  iterable: Iterable.

        """
        group = canvas.Group(*[task.ActivityTask(activity, *i) for i in iterable])
        return self.submit(group).futures

    def fail(self, reason, details=None, wfargs=None, wfkwargs=None):
        self._executor.fail(reason, details, wfargs, wfkwargs)

    def before_run(self, history):
        pass

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def after_run(self, history):
        pass

    def on_failure(self, history, reason, details=None, wfargs=None, wfkwargs=None):
        """
        The executor calls this method when the workflow fails.

        """
        pass

    def on_start(self, wfargs=None, wfkwargs=None):
        pass

    def on_complete(self, result, history, wfargs=None, wfkwargs=None):
        pass
