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
    def __init__(self, domain, task_list, workflow, heartbeat=60, soft_cancel_wait_period=180,
                 *args, **kwargs):
        self._workflow = workflow
        self.nb_retries = 3
        self._heartbeat = heartbeat
        self._soft_cancel_wait_period = soft_cancel_wait_period
        self.is_alive = True
        self.is_shutdown = Event()

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
        run_in_proc(self, token, task, self._heartbeat, self._soft_cancel_wait_period, self.is_shutdown)

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
        except Exception as err:
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
            result = ActivityTask(activity, self.is_shutdown, *args, **kwargs).execute()
        except TaskCancelled:
            raise
        except Exception as err:
            tb = traceback.format_exc()
            logger.exception(err)
            return poller.fail(token, task, reason=str(err), details=tb)

        try:
            poller._complete(token, json.dumps(result))
        except Exception as err:
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

def registerTaskCancelHandler(isTaskFinished, poller, task):
    def signal_task_cancellation(signum, frame):
        logger.info(
            '[SWF][Worker] Signal %d caught. Sending TaskCancelled exception from %s. Task: [%s]',
            signum,
            poller.identity,
            task.activity_type.name
        )

        if not isTaskFinished.is_set():
            if signum == signal.SIGUSR1:
                raise SoftTaskCancelled(task)
            elif signum == signal.SIGUSR2:
                raise TaskCancelled(task)

    signal.signal(signal.SIGUSR1, signal_task_cancellation)
    signal.signal(signal.SIGUSR2, signal_task_cancellation)

def run_in_proc(poller, token, task, heartbeat=60, soft_cancel_wait_period=180, is_shutdown=None):
    pid = os.getpid()
    isTaskFinished = threading.Event()

    registerTaskCancelHandler(isTaskFinished, poller, task)

    # start the heartbeat thread
    heartbeat_thread = threading.Thread(target=start_heartbeat, args=(poller, token, task, isTaskFinished, heartbeat, pid, soft_cancel_wait_period))
    heartbeat_thread.setDaemon(True)
    heartbeat_thread.start()

    isTaskCancelled = False
    # start processing the task
    try:
        try:
            logger.info('[SWF][Worker] Start processing task. Task: [%s].', task.activity_type.name)
            process_task(poller, token, task, is_shutdown)
            logger.info('[SWF][Worker] Finished processing task. Task: [%s].', task.activity_type.name)
        except TaskCancelled:
            # task is cancelled by the heartbeat thread from swf
            isTaskCancelled = True

        finally:
            # task finished. Let's finish the heartbeat thread
            isTaskFinished.set()
            # let's wait for the heartbeat thread to die
            heartbeat_thread.join()
    except TaskCancelled:
        # race condition to prevent TaskCancelled exception raised in the finally block above
        isTaskCancelled = True

    finally:
        # task finished. Let's finish the heartbeat thread
        isTaskFinished.set()
        # let's wait for the heartbeat thread to die
        heartbeat_thread.join()

    if isTaskCancelled:
        logger.info('[SWF][Worker][Worker] Reporting task is cancelled. Task: [%s].', task.activity_type.name)

        try:
            poller.cancel(token)
        except:
            logger.info('[SWF][Worker][Worker] Failed to send task cancel confirmation to swf. Ignore. Task: [%s].', task.activity_type.name)

    logger.info('[SWF][Worker][Heartbeat] Heartbeat thread stopped. Task: [%s]', task.activity_type.name)


def start_heartbeat(poller, token, task, isTaskFinished, heartbeat, pid, soft_cancel_wait_period):

    cancel_requested_at = [None]

    def try_cancel_task():

        if cancel_requested_at[0] == None:
            # first time. Send a soft cancel
            logger.info('[SWF][Worker][Heartbeat] Sending signal SIGUSR1 for soft cancellation. Task: [%s].', task.activity_type.name)
            os.kill(int(pid), signal.SIGUSR1)
            cancel_requested_at[0] = datetime.utcnow()
        else:
            # we had sent a cancellation before.
            now = datetime.utcnow()

            if (now - cancel_requested_at[0]).total_seconds() >= soft_cancel_wait_period:
                # time's up. Send a hard cancellation
                logger.info('[SWF][Worker][Heartbeat] Sending signal SIGUSR2 for hard cancellation. Task: [%s].', task.activity_type.name)
                os.kill(int(pid), signal.SIGUSR2)
            else:
                logger.info('[SWF][Worker][Heartbeat] Soft cancellation sent at [%s]. Waiting the task to be cancelled. Task: [%s].', cancel_requested_at[0].isoformat(), task.activity_type.name)

    while (not isTaskFinished.is_set()):
        isTaskFinished.wait(heartbeat)
        if (not isTaskFinished.is_set()):
            # task is still running
            logger.info('[SWF][Worker][Heartbeat] Sending heartbeat. Task: [%s].', task.activity_type.name)

            response = None

            try:
                response = poller.heartbeat(token)
            except swf.exceptions.DoesNotExistError:
                # The workflow no long exist. It may timed out. Kill the process.
                logger.info('[SWF][Worker][Heartbeat] Activity DoesNotExistError when sending heartbeat. Stopping the task. Task: [%s].', task.activity_type.name)
                try_cancel_task()

            except Exception as error:
                # Ignore if we failed to send heartbeat. The
                # subprocess will become orphan and the heartbeat timeout may
                # eventually trigger on Amazon SWF side.
                logger.error('[SWF][Worker][Heartbeat] Cannot send heartbeat for task {}: {}'.format(
                    task.activity_type.name,
                    error))

            if response and response.get('cancelRequested'):
                # Task cancelled.
                logger.info('[SWF][Worker][Heartbeat] Activity received cancallaton request. Stopping the task. Task: [%s].', task.activity_type.name)
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
        except Exception as error:
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
