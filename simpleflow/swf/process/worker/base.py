import json
import logging
import multiprocessing
import os
import psutil
import signal
import traceback

import swf.actors
import swf.format

import simpleflow
from simpleflow.swf.process.actor import (
    Supervisor,
    Poller,
    with_state,
)
from simpleflow.swf.task import ActivityTask
from .dispatch import from_task_registry
import threading
from simpleflow.exceptions import SoftTaskCancelled
from simpleflow.exceptions import TaskCancelled
from datetime import datetime
from threading import Event
import uuid
from contextlib import contextmanager
import socket
from simpleflow.settings import default

logger = logging.getLogger(__name__)


class Worker(Supervisor):
    def __init__(self, poller, nb_children=None):
        self._poller = poller
        self._poller.is_alive = True
        Supervisor.__init__(
            self,
            payload=self._poller.start,
            nb_children=nb_children,
        )


class ActivityPoller(Poller, swf.actors.ActivityWorker):
    """
    Polls an activity and handles it in the worker.

    """
    def __init__(self, domain, task_list, workflow, heartbeat=60, max_restart_count=3000, max_RSS_restart=5000, socket_timeout=120, *args, **kwargs):
        self._workflow = workflow
        self.nb_retries = 3
        self._heartbeat = heartbeat
        self.is_alive = True
        self.is_shutdown = Event()
        self.max_restart_count = max_restart_count
        self.max_RSS_restart = max_RSS_restart
        self.socket_timeout = socket_timeout

        swf.actors.ActivityWorker.__init__(
            self,
            domain,
            task_list,
            *args,    # directly forward them.
            **kwargs  # directly forward them.
        )

    @property
    def name(self):
        return '{}'.format(
            self.__class__.__name__
        )

    @with_state('polling')
    def poll(self, task_list, identity):
        return swf.actors.ActivityWorker.poll(self, task_list, identity)

    @with_state('processing task')
    def process(self, request):
        token, task = request
        activityId = uuid.uuid4()
        init_thread_local(task, activityId)
        return run_in_proc(self, token, task, activityId, self._heartbeat, self.is_shutdown, self.max_RSS_restart, self.socket_timeout)

    @with_state('completing')
    def complete(self, token, result):
        swf.actors.ActivityWorker.complete(self, token, result)

    @with_state('failing')
    def fail(self, token, task, reason=None, details=None):
        try:
            return swf.actors.ActivityWorker.fail(
                self,
                token,
                reason=swf.format.reason(reason),
                details=swf.format.details(details),
            )
        except swf.exceptions.DoesNotExistError:
            logger.info('cannot fail task {}: {}'.format(
                task.activity_type.name,
                err,
            ))
        except BaseException as err:
            logger.error('cannot fail task {}: {}'.format(
                task.activity_type.name,
                err,
            ))

    @with_state('cancelling')
    def cancel(self, token, details=None):
        swf.actors.ActivityWorker.cancel(self, token, details)


class ActivityWorker(object):
    def __init__(self, workflow, is_shutdown=None):
        self._dispatcher = from_task_registry.RegistryDispatcher(
            simpleflow.registry.registry,
            None,
            workflow,
        )

        self.is_shutdown = is_shutdown

    def dispatch(self, task):
        name = task.activity_type.name
        return self._dispatcher.dispatch_activity(name)

    def process(self, poller, token, task):
        try:
            logger.debug('ActivityWorker.porcess() pid={}'.format(os.getpid()))
            activity = self.dispatch(task)
            input = json.loads(task.input)
            args = input.get('args', ())
            kwargs = input.get('kwargs', {})
            result = ActivityTask(activity, is_shutdown=self.is_shutdown, *args, **kwargs).execute()
        except BaseException as err:
            tb = traceback.format_exc()
            logger.exception(err)
            return poller.fail(token, task, reason=unicode(err), details=tb)

        try:
            poller._complete(token, json.dumps(result))
        except swf.exceptions.DoesNotExistError:
            logger.info('cannot complete task {} due to DoesNotExistError' % task.activity_id)
        except BaseException as err:
            tb = traceback.format_exc()
            reason = 'cannot complete task {}: {} {}'.format(
                task.activity_id,
                err,
                tb
            )
            logger.exception(reason)
            poller.fail(token, task, reason)


def process_task(poller, token, task, is_shutdown):
    logger.debug('process_task() pid={}'.format(os.getpid()))
    worker = ActivityWorker(poller._workflow, is_shutdown)
    worker.process(poller, token, task)


def monitor_child(pid, info):
    def _handle_child_exit(signum, frame):
        if signum == signal.SIGCHLD:
            # Only fill the info dict. The spawn() function calls
            # ``worker.join()`` to collect the subprocess when it exits.
            try:
                _, status = os.waitpid(pid, 0)
            except OSError:
                # Beware of the race between this handler and
                # :meth:`Process.join`. This is the main reason to raise a
                # ``errno 10: No child processe``.
                return
            sig = status & 0xff
            exit_code = (status & 0xff00) >> 8
            info['signal'] = sig
            info['exit_code'] = exit_code

    signal.signal(signal.SIGCHLD, _handle_child_exit)

def init_thread_local(task, activityId):
    from simpleflow.swf.process.actor import thread_local

    thread_local.activity_id = activityId
    thread_local.workflow_input = getattr(task, 'input', '')

def registerTaskCancelHandler(isTaskFinished, poller, task_input):
    def signal_task_cancellation(signum, frame):
        logger.info(
            '[SWF][Worker] Signal %d caught. Sending TaskCancelled exception from %s. Task: [%s]',
            signum,
            poller.identity,
            task_input
        )

        if not isTaskFinished.is_set():
            if signum == signal.SIGUSR1:
                raise SoftTaskCancelled(task_input)
            elif signum == signal.SIGUSR2:
                raise TaskCancelled(task_input)

    signal.signal(signal.SIGUSR1, signal_task_cancellation)
    signal.signal(signal.SIGUSR2, signal_task_cancellation)


def kill_all_children():
    # kill any leftover child processes
    try:
        us = os.getpid()
        p = psutil.Process(us)
        for child in p.children(recursive=True):
            logger.info('killing children: %s' % child.__dict__)
            child.kill()
            child.wait(5)
    except:
        logger.info("Killing child processes failed. %s" % traceback.format_exc())


def hfloat(f, p=5):
    """Convert float to value suitable for humans.
    :keyword p: Float precision.
    """
    i = int(f)
    return float(i if i == f else '{0:.{p}}'.format(f, p=p))


def humanMB(s):
    """Convert bytes to MB."""
    return hfloat(s / (2 ** 20.0))

def get_memory_usage():

    try:
        p = psutil.Process(os.getpid())
        return humanMB(p.memory_info().rss)
    except:
        logger.info('get_memory_usage failed. %s' % traceback.format_exc())


def cleanup_memory():
    try:
        import gc
        gc.collect()
    except:
        logger.info('cleanup_memory failed. %s' % traceback.format_exc())

@contextmanager
def default_socket_timeout(timeout):
    prev = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout)
    yield
    socket.setdefaulttimeout(prev)


def run_in_proc(poller, token, task, activity_id, heartbeat=60, is_shutdown=None, max_RSS_restart=5000, socket_timeout=120):
    to_restart = False

    pid = os.getpid()
    isTaskFinished = threading.Event()

    registerTaskCancelHandler(isTaskFinished, poller, task.input)

    # start the heartbeat thread
    heartbeat_thread = threading.Thread(target=start_heartbeat, args=(poller, token, task, isTaskFinished, heartbeat, pid, activity_id))
    heartbeat_thread.setDaemon(True)
    heartbeat_thread.start()

    rss_before = get_memory_usage()

    logger.info('RAM - RSS before running task: %sMB' % rss_before)

    # start processing the task
    try:
        try:
            logger.info('[SWF][Worker][%s] Start processing task. ', task.activity_type.name)

            with default_socket_timeout(socket_timeout):
                process_task(poller, token, task, is_shutdown)

            logger.info('[SWF][Worker][%s] Finished processing task. ', task.activity_type.name)

        except:
            logger.error('[SWF][Worker][{}] Error when processing task. Exception: {}'.format(task.activity_type.name, traceback.format_exc()))
            raise

        finally:
            # task finished. Let's finish the heartbeat thread
            isTaskFinished.set()
            # let's wait for the heartbeat thread to die
            heartbeat_thread.join()

    except:
        logger.error('[SWF][Worker][{}] Error when processing task. Exception: {}'.format(task.activity_type.name, traceback.format_exc()))
        raise

    finally:
        # task finished. Let's finish the heartbeat thread
        isTaskFinished.set()
        # let's wait for the heartbeat thread to die
        heartbeat_thread.join()

        kill_all_children()

        cleanup_memory()

        rss_after = get_memory_usage()
        rss_delta = rss_after - rss_before

        logger.info('RAM - RSS delta for this activity: %sMB. Current RSS: %sMB' % (rss_delta, rss_after))

        if rss_after > max_RSS_restart:
            logger.info('RAM - RSS after GC collect is beyond %sMB. Restarting the worker.' % max_RSS_restart)
            to_restart = True


    logger.info('[SWF][Worker][%s] Heartbeat thread stopped. ', task.activity_type.name)
    return to_restart


def start_heartbeat(poller, token, task, isTaskFinished, heartbeat, pid, activity_id):
    init_thread_local(task, activity_id)

    started_at = datetime.utcnow()
    input = json.loads(task.input)
    kwargs = input.get('kwargs', {})
    taskId = kwargs.get('taskId', None)
    taskName = kwargs.get('taskName', None)
    task_timeout = int(kwargs.get('task_start_to_close_timeout', default.ACTIVITY_DEFAULT_TIMEOUT))

    task_identity = "TaskId: %s. TaskName: %s." % (taskId, taskName)

    # [soft_timeout_requested_time, hard_timeout_requested_time]
    cancel_requested_at = [None, None]

    def try_cancel_task():

        if cancel_requested_at[0] == None:
            # first time. Send a soft cancel
            logger.info('[SWF][Worker][Heartbeat][%s] Sending signal SIGUSR1 for soft cancellation. ', task_identity)
            os.kill(int(pid), signal.SIGUSR1)
            cancel_requested_at[0] = datetime.utcnow()
        else:
            # we had sent a cancellation before.
            now = datetime.utcnow()

            if (now - cancel_requested_at[0]).total_seconds() >= int(default.ACTIVITY_SOFT_TIMEOUT_BUFFER):
                # time's up. Send a hard cancellation

                if cancel_requested_at[1] == None:
                    logger.info('[SWF][Worker][Heartbeat][%s] Sending signal SIGUSR2 for hard cancellation. ', task_identity)
                    logger.info('[SWF][Worker][%s] Task hit hard timeout limit. Task will be forced exited. ', task_identity)

                    os.kill(int(pid), signal.SIGUSR2)
                    cancel_requested_at[1] = datetime.utcnow()
                else:
                    logger.info('[SWF][Worker][Heartbeat][%s] Hard cancellation sent at [%s]. Waiting the task to be cancelled. ', task_identity, cancel_requested_at[1].isoformat())

                    # exit the process after 30s the hard timeout is sent
                    if (now - cancel_requested_at[1]).total_seconds() >= 30:
                        logger.info('[SWF][Worker][Heartbeat][%s] Hard cancellation sent at [%s]. Now forcing exiting the worker process. ', task_identity, cancel_requested_at[1].isoformat())

                        os.kill(int(pid), signal.SIGKILL)

            else:
                logger.info('[SWF][Worker][Heartbeat][%s] Soft cancellation sent at [%s]. Waiting the task to be cancelled. ', task_identity, cancel_requested_at[0].isoformat())

    while (not isTaskFinished.is_set()):
        isTaskFinished.wait(heartbeat)

        task_duration = (datetime.utcnow() - started_at).total_seconds()
        if (task_duration >= task_timeout):
            # soft timeout. Proactively killing the task
            logger.info('[SWF][Worker][Heartbeat][%s] Task has been running for %s seconds. Stopping the task.', task_identity, task_duration)
            try_cancel_task()

        if (not isTaskFinished.is_set()):
            # task is still running
            logger.debug('[SWF][Worker][Heartbeat][%s] Sending heartbeat. ', task_identity)

            response = None

            try:
                response = poller.heartbeat(token)
            except swf.exceptions.DoesNotExistError:
                # The workflow no long exist. It may timed out. Kill the process.
                logger.info('[SWF][Worker][Heartbeat][%s] Activity DoesNotExistError when sending heartbeat. Stopping the task.', task_identity)
                try_cancel_task()

            except BaseException as error:
                # Ignore if we failed to send heartbeat. The
                # subprocess will become orphan and the heartbeat timeout may
                # eventually trigger on Amazon SWF side.
                logger.error('[SWF][Worker][Heartbeat][{}] Cannot send heartbeat. {}'.format(
                    task_identity,
                    error))

            if response and response.get('cancelRequested'):
                # Task cancelled.
                logger.info('[SWF][Worker][Heartbeat][%s] Activity received cancallaton request. Stopping the task.', task_identity)
                try_cancel_task()


def spawn(poller, token, task, heartbeat=60):
    logger.debug('spawn() pid={}'.format(os.getpid()))
    worker = multiprocessing.Process(
        target=process_task,
        args=(poller, token, task),
    )
    worker.start()

    info = {}
    monitor_child(worker.pid, info)

    def worker_alive(): psutil.pid_exists(worker.pid)
    while worker_alive():
        worker.join(timeout=heartbeat)
        if not worker_alive():
            if worker.exitcode != 0:
                poller.fail(
                    token,
                    task,
                    reason='process died: signal {}, exit code{}'.format(
                        info.get('signal'),
                        info.get('exit_code', worker.exitcode),
                    ))
            return
        try:
            response = poller.heartbeat(token)
        except swf.exceptions.DoesNotExistError:
            # The subprocess is responsible for completing the task.
            # Either the task or the workflow execution no longer exists.
            return
        except BaseException as error:
            # Let's crash if it cannot notify the heartbeat failed.  The
            # subprocess will become orphan and the heartbeat timeout may
            # eventually trigger on Amazon SWF side.
            logger.error('cannot send heartbeat for task {}: {}'.format(
                task.activity_type.name,
                error))
            raise

        if response and response.get('cancelRequested'):
            # Task cancelled.
            worker.terminate()  # SIGTERM
            return
