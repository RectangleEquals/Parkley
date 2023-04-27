import threading


class FunctionThread(threading.Thread):
    def __init__(self, target, onerror=None, args=(), kwargs=None):
        super().__init__(target=target, args=args, kwargs=kwargs)
        self.onerror = onerror
        self._stop_event = threading.Event()
        self._completed_event = threading.Event()
        self.exception = None

    def run(self):
        if self.exception:
            self.exception = None
        try:
            self._target(*self._args, **self._kwargs)
        except Exception as e:
            self.exception = e
            if self.onerror:
                self.onerror()
        finally:
            if not self.exception:
                self._completed_event.set()
                """
            else:
                self.join()
                print('Joined thread')
                """

    def stop(self):
        self._stop_event.set()

    def is_stopped(self):
        return self._stop_event.is_set()

    def is_running(self):
        return self.is_alive() and not self.is_stopped() and not self.is_completed()

    def is_completed(self):
        return self._completed_event.is_set()

    def has_exception(self):
        return self.exception is not None

    def get_last_exception(self):
        return self.exception
