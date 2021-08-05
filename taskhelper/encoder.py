
import nbt as NBT
import traceback

from .task import Task

class EncodeTask(Task):
  def __init__(self, src, drt, **kwargs):
    super().__init__(**kwargs)
    self._src = src
    self._drt = drt

  @property
  def src(self):
    return self._src

  @property
  def drt(self):
    return self._drt

class EncodeNbtTask(EncodeTask):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  def run(self):
    try:
      jobj = NBT.nbtToJson(NBT.NBTFile(filename=self.src))
      with open('{}.jnbt'.format(self.drt), 'w') as fd:
        fd.write(jobj)
      return True, self.src, self.drt
    except:
      print('Encode nbt error:', traceback.format_exc())
      return False, self.src, self.drt

class EncodeRegTask(EncodeTask):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  def run(self):
    try:
      reg_file = NBT.RegionFile(filename=self.src)
      with open('{drt}.jreg'.format(drt=self.drt), 'w') as fd:
        chunks = NBT.TAG_Compound()
        for m in reg_file.get_metadata():
          chunk = reg_file.get_nbt(m.x, m.z)
          chunk.name = hex(m.x + m.z * 32)[2:]
          chunks.tags.append(chunk)
        fd.write(NBT.nbtToJson(chunks))
      return True, self.src, self.drt
    except:
      print('Encode reg error:', traceback.format_exc())
      return False, self.src, self.drt
