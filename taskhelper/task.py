
import multiprocessing
try:
  from multiprocessing.context import ForkProcess as Process
except ImportError:
  from multiprocessing.context import SpawnProcess as Process
from threading import Thread

__all__ = [
  'Task', 'HelperProcess'
]

class Task:
  def __init__(self):
    pass

  def run(self):
    raise RuntimeError()

  def start(self, queue):
    re = self.run()
    queue.put(re)

class HelperProcess(Process):
  EXIT_CODE = 0x00

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self._isclose = False
    self._tasks = multiprocessing.Queue()
    self._task_return = multiprocessing.Queue()
    self.__threads = []

  def run(self):
    try:
      while not self._isclose:
        task = self._tasks.get()
        if task == HelperProcess.EXIT_CODE:
          break
        t = Thread(target=lambda: task.start(self._task_return), name='gbu-helper-thr-{}'.format(task.__class__.__name__))
        t.start()
        self.__threads.append(t)
    except KeyboardInterrupt:
      pass
    finally:
      self.close()

  def close(self):
    if self._isclose:
      return
    self._isclose = True
    threads = self.__threads
    self.threads = []
    for t in threads:
      if t.is_alive():
        t.join(timeout=0)
    if not self._tasks._closed:
      self._tasks.put(HelperProcess.EXIT_CODE)

  __del__ = close

  def run_task(self, task):
    assert isinstance(task, Task)
    self._tasks.put(task)
    self._tasks._poll()

  def wait_task(self):
    re = self._task_return.get()
    return re

  def _flush_threads(self):
    for i, t in enumerate(self.__threads):
      if not t.is_alive():
        self.__threads.pop(i)
