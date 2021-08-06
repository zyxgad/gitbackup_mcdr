
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

  def run(self):
    try:
      while not self._isclose:
        task = self._tasks.get()
        if task == HelperProcess.EXIT_CODE:
          break
        t = Thread(target=lambda: task.start(self._task_return),
          name='gbu-helper-thr-{}'.format(task.__class__.__name__), daemon=True)
        t.start()
    except KeyboardInterrupt:
      pass
    finally:
      self.exit()
      if not self._tasks._closed: self._tasks.exit()
      if not self._task_return._closed: self._task_return.exit()

  def exit(self):
    if self._isclose:
      return
    self._isclose = True
    if not self._tasks._closed:
      self._tasks.put(HelperProcess.EXIT_CODE)

  __del__ = exit

  def run_task(self, task):
    assert isinstance(task, Task)
    self._tasks.put(task)
    self._tasks._poll()

  def wait_task(self):
    re = self._task_return.get()
    return re

  def task_empty(self):
    return self._task_return.empty()

