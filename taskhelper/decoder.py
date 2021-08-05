
import nbt as NBT
import traceback

from .task import Task

class DecodeTask(Task):
  def __init__(self, src, drt, **kwargs):
    super().__init__(**kwargs)
    self._src = src
    self._drt = drt

  def _run(self):
    raise RuntimeError()

  def run(self):
    re = self._run()
    return re

  @property
  def src(self):
    return self._src

  @property
  def drt(self):
    return self._drt

class DecodeNbtTask(DecodeTask):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  def _run(self):
    try:
      with open(self.src, 'r') as fd:
        nbt_ = NBT.jsonToNbt(fd.read())
        nbt_.write_file(filename=self.drt)
      return True, self.src, self.drt
    except:
      print('Decode nbt error:', traceback.format_exc())
      return False, self.src, self._drt

  @property
  def drt(self):
    return self._drt[:-5] if self._drt.endswith('.jnbt') else self._drt

class DecodeRegTask(DecodeTask):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  def run(self):
    try:
      with open(self.src, 'r') as fd:
        chunks = NBT.jsonToNbt(fd.read()).tags
        with open(self.drt, 'w+b') as reg_fd:
          reg_file = NBT.RegionFile(fileobj=reg_fd)
          for c in chunks:
            x_z = int(c.name, 16)
            x, z = x_z % 32, x_z // 32
            reg_file.write_chunk(x, z, c)
      return True, self.src, self.drt
    except:
      print('Decode reg error:', traceback.format_exc())
      return False, self.src, self._drt

  @property
  def drt(self):
    return self._drt[:-5] if self._drt.endswith('.jreg') else self._drt
