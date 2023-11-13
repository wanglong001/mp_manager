from concurrent.futures import ProcessPoolExecutor, Future, _base, process
from concurrent.futures._base import Future
import queue
import psutil
import os
import multiprocessing 
import multiprocessing as mp
import traceback
import time

class ExecutorManagerThread(process._ExecutorManagerThread):

    def wait_result_broken_or_wakeup(self):
        # Wait for a result to be ready in the result_queue while checking
        # that all worker processes are still running, or for a wake up
        # signal send. The wake up signals come either from new tasks being
        # submitted, from the executor being shutdown/gc-ed, or from the
        # shutdown of the python interpreter.
        result_reader = self.result_queue._reader
        assert not self.thread_wakeup._closed
        wakeup_reader = self.thread_wakeup._reader
        readers = [result_reader, wakeup_reader]
        worker_sentinels = [p.sentinel for p in list(self.processes.values())]
        ready = mp.connection.wait(readers + worker_sentinels)

        cause = None
        is_broken = True
        result_item = None
        if result_reader in ready:
            try:
                result_item = result_reader.recv()
                is_broken = False
            except BaseException as e:
                cause = traceback.format_exception(type(e), e, e.__traceback__)
                is_broken = False
                result_item = None

        elif wakeup_reader in ready:
            is_broken = False
        else:
            is_broken = False
            result_item = None

        with self.shutdown_lock:
            self.thread_wakeup.clear()

        return result_item, is_broken, cause

    def run(self):
        # Main loop for the executor manager thread.

        while True:
            self.add_call_item_to_queue()

            result_item, is_broken, cause = self.wait_result_broken_or_wakeup()

            if is_broken:
                self.terminate_broken(cause)
                return
            if result_item is not None:
                self.process_result_item(result_item)
                # Delete reference to result_item to avoid keeping references
                # while waiting on new results.
                del result_item

                # attempt to increment idle process count
                executor = self.executor_reference()
                if executor is not None:
                    executor._idle_worker_semaphore.release()
                del executor

            if self.is_shutting_down():
                self.flag_executor_shutting_down()

                # Since no new work items can be added, it is safe to shutdown
                # this thread if there are no pending work items.
                for key in list(self.pending_work_items.keys()):
                    if self.pending_work_items[key].future._state == _base.CANCELLED:
                        self.pending_work_items.pop(key)
                if not self.pending_work_items:
                    self.join_executor_internals()
                    return

class ProcessFuture(Future):

    def __init__(self) -> None:
        super().__init__()
        self.pid = multiprocessing.Manager().Value("i", -1)
        self._condition = multiprocessing.Manager().Condition()
    
    def set_exception(self, exception):
        """Sets the result of the future as being the given exception.

        Should only be used by Executor implementations and unit tests.
        """
        with self._condition:
            self._exception = exception
            self._state = _base.FINISHED
            for waiter in self._waiters:
                waiter.add_exception(self)
            self._condition.notify_all()
        self._invoke_callbacks()
    
    def set_pid(self, pid):
        self.pid.set(pid)

    def set_result(self, result):
        """Sets the return value of work associated with the future.

        Should only be used by Executor implementations and unit tests.
        """
        with self._condition:
            if self._state in {_base.CANCELLED_AND_NOTIFIED, _base.FINISHED}:
                raise _base.InvalidStateError('{}: {!r}'.format(self._state, self))
            self._result = result
            self._state = _base.FINISHED
            for waiter in self._waiters:
                waiter.add_result(self)
            self._condition.notify_all()
        self._invoke_callbacks()
    
    def result(self, timeout=None):
        if self._state == _base.CANCELLED:
            return None
        return super().result(timeout)
    
    def _wait_running(self):
        try_num = 0
        while try_num < 5:
            if self.pid.get() != -1:
                break
            time.sleep(1)
            try_num+=1
        else:
            raise Exception("ProcessFuture: self.pid is None")

    def cancel(self) -> bool:
        """Cancel the future if possible.

        Returns True if the future was cancelled, False otherwise. A future
        cannot be cancelled if it is running or has already completed.
        """
        with self._condition:
            if self._state in [_base.FINISHED]:
                return False

            if self._state in [_base.CANCELLED, _base.CANCELLED_AND_NOTIFIED]:
                return True
            
            if self._state == _base.RUNNING:
                self._wait_running()
                psutil.Process(self.pid.get()).kill()
                # self.set_exception(Exception("ProcessFuture: task has been canceled"))
                

            self._state = _base.CANCELLED
            self._condition.notify_all()

        self._invoke_callbacks()
        return 

    def suspend(self):
        with self._condition:
            if self._state == _base.RUNNING:
                self._wait_running()
                psutil.Process(self.pid.get()).suspend()

    def resume(self):
        with self._condition:
            if self._state == _base.RUNNING:
                self._wait_running()
                psutil.Process(self.pid.get()).resume()

class ProcessPool(ProcessPoolExecutor):
    
    def submit(self, fn, /, *args, **kwargs):
        with self._shutdown_lock:
            if self._broken:
                raise process.BrokenProcessPool(self._broken)
            if self._shutdown_thread:
                raise RuntimeError('cannot schedule new futures after shutdown')
            if process._global_shutdown:
                raise RuntimeError('cannot schedule new futures after '
                                   'interpreter shutdown')

            f = ProcessFuture()
            kwargs['__future'] =  f
            w = process._WorkItem(f, fn, args, kwargs)

            self._pending_work_items[self._queue_count] = w
            self._work_ids.put(self._queue_count)
            self._queue_count += 1
            # Wake up queue management thread
            self._executor_manager_thread_wakeup.wakeup()

            self._adjust_process_count()
            self._start_executor_manager_thread()
            return f

    def _start_executor_manager_thread(self):
        if self._executor_manager_thread is None:
            # Start the processes so that their sentinels are known.
            self._executor_manager_thread = ExecutorManagerThread(self)
            self._executor_manager_thread.start()
            process._threads_wakeups[self._executor_manager_thread] = \
                self._executor_manager_thread_wakeup

    def _adjust_process_count(self):
        for pid in list(self._processes.keys()):
            if not self._processes[pid].is_alive():
                del self._processes[pid]
                self._idle_worker_semaphore.release()
        # if there's an idle process, we don't need to spawn a new one.
        if self._idle_worker_semaphore.acquire(blocking=False):
            return
        process_count = len(self._processes)
        if process_count < self._max_workers:
            p = self._mp_context.Process(
                target=_process_worker,
                args=(self._call_queue,
                      self._result_queue,
                      self._initializer,
                      self._initargs))
            p.start()
            self._processes[p.pid] = p
        
def _process_worker(call_queue, result_queue, initializer, initargs):
    """Evaluates calls from call_queue and places the results in result_queue.

    This worker is run in a separate process.

    Args:
        call_queue: A ctx.Queue of _CallItems that will be read and
            evaluated by the worker.
        result_queue: A ctx.Queue of _ResultItems that will written
            to by the worker.
        initializer: A callable initializer, or None
        initargs: A tuple of args for the initializer
    """
    if initializer is not None:
        try:
            initializer(*initargs)
        except BaseException:
            _base.LOGGER.critical('Exception in initializer:', exc_info=True)
            # The parent will notice that the process stopped and
            # mark the pool broken
            return
    while True:
        try:
            call_item = call_queue.get(block=False)
        except:
            import time
            time.sleep(1)
            return
        if call_item is None:
            # Wake up queue management thread
            result_queue.put(os.getpid())
            return
        try:
            f = call_item.kwargs["__future"]
            del call_item.kwargs["__future"]

            f.set_pid(os.getpid())
            r = call_item.fn(*call_item.args, **call_item.kwargs)
        except BaseException as e:
            exc = process._ExceptionWithTraceback(e, e.__traceback__)
            process._sendback_result(result_queue, call_item.work_id, exception=exc)
        else:
            process._sendback_result(result_queue, call_item.work_id, result=r)
            del r

        # Liberate the resource as soon as possible, to avoid holding onto
        # open files or shared memory that is not needed anymore
        del call_item

setattr(process, "_process_worker", _process_worker)

