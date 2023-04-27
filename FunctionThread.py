import threading
import ctypes


class FunctionThread(threading.Thread):
    def __init__(self, target, on_error=None, on_done=None, args=(), kwargs=None):
        super().__init__(target=target, args=args, kwargs=kwargs)
        self.on_error = on_error
        self.on_done = on_done
        self.exception = None
        self._stop_event = threading.Event()
        self._done_event = threading.Event()

    def __del__(self):
        self.stop(True)

    def run(self):
        self.exception = None
        self._stop_event.clear()
        self._done_event.clear()

        try:
            self._target(*self._args, **self._kwargs)
        except Exception as e:
            self.stop()
            self.exception = e
            if self.on_error:
                self.on_error(e)
        finally:
            if not self.has_exception() and not self._stop_event.is_set():
                self._done_event.set()
            if self.on_done:
                self.on_done()

    def get_id(self):
        if hasattr(self, '_thread_id'):
            return self._thread_id
        for id, thread in threading._active.items():
            if thread is self:
                return id

    def stop(self, force=False):
        self._done_event.clear()
        if not force:
            self._stop_event.set()
        else:
            thread_id = self.get_id()
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.py_object(SystemExit))
            if res > 1:
                self.exception = res
                ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)

    def is_running(self):
        return self.is_alive()

    def is_stopping(self):
        return self._stop_event.is_set()

    def is_done(self):
        return self._done_event.is_set()

    def has_exception(self):
        return self.exception is not None

    def get_last_exception(self):
        return self.exception
