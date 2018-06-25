import swf.models

from simpleflow import task
from simpleflow.settings import default


class ActivityTask(task.ActivityTask):
    def schedule(self, domain, task_list=None, **kwargs):
        activity = self.activity
        # Always involve a GET call to the SWF API which introduces useless
        # latency if the ActivityType already exists.
        model = swf.models.ActivityType(
            domain,
            activity.name,
            version=activity.version,
        )

        input = {
            'args': self.args,
            'kwargs': self.kwargs,
        }

        if task_list is None:
            task_list = activity.task_list
        task_timeout = kwargs.get(
            'task_timeout',
            str(activity.task_start_to_close_timeout),
        )
        duration_timeout = kwargs.get(
            'duration_timeout',
            str(activity.task_schedule_to_close_timeout),
        )
        schedule_timeout = kwargs.get(
            'schedule_timeout',
            str(activity.task_schedule_to_start_timeout),
        )
        heartbeat_timeout = kwargs.get(
            'heartbeat_timeout',
            str(activity.task_heartbeat_timeout),
        )
        task_priority = kwargs.get('priority', str(self.task_priority))

        # check if task_start_to_close_timeout is specified in the task message (task input)
        task_timeout_override = self.kwargs.get('task_start_to_close_timeout', None)

        if task_timeout_override != None:
            task_timeout_int = int(task_timeout_override) + int(default.ACTIVITY_SOFT_TIMEOUT_BUFFER) + int(default.ACTIVITY_HARD_TIMEOUT_BUFFER)
            duration_timeout_int = str(task_timeout_int + int(default.ACTIVITY_SCHEDULE_TO_START_TIMEOUT))

            task_timeout = str(task_timeout_int)
            duration_timeout = str(duration_timeout_int)

        decision = swf.models.decision.ActivityTaskDecision(
            'schedule',
            activity_id=self.id,
            activity_type=model,
            control=None,
            task_list=task_list,
            input=input,
            task_timeout=task_timeout,
            duration_timeout=duration_timeout,
            schedule_timeout=schedule_timeout,
            heartbeat_timeout=heartbeat_timeout,
            task_priority=task_priority,
        )

        return [decision]



class WorkflowTask(task.WorkflowTask):
    def schedule(self, domain, task_list=None):
        workflow = self.workflow
        # Always involve a GET call to the SWF API which introduces useless
        # latency if the WorkflowType already exists.
        model = swf.models.WorkflowType(
            domain,
            workflow.name,
            version=workflow.version,
        )

        input = {
            'args': self.args,
            'kwargs': self.kwargs,
        }

        decision = swf.models.decision.ChildWorkflowExecutionDecision(
            'start',
            workflow_id=self.id,
            workflow_type=model,
            task_list=workflow.task_list,
            input=input,
            tag_list=workflow.tag_list,
            child_policy=workflow.child_policy,
            execution_timeout=str(workflow.execution_timeout))

        return [decision]
