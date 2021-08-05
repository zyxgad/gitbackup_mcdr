
import os
import hashlib
import gzip
import nbt as NBT
import traceback

from .task import Task

def get_file_hash(src, chunk=1024 * 1024):
  sha = hashlib.sha256()
  with open(src, 'rb') as fd:
    while True:
      c = fd.read(chunk)
      if not c:
        break
      sha.update(c)
  return sha.hexdigest()

class EncodeTask(Task):
  def __init__(self, src, drt, **kwargs):
    super().__init__(**kwargs)
    self._src = src
    self._drt = drt

  def _run(self):
    raise RuntimeError()

  def run(self):
    hash_file = self.drt + '.hash.gbu'
    cache_file = self.drt + '.cache.gbu'
    mtime = int(os.stat(self.src).st_mtime)
    hash_ = get_file_hash(self.src)
    if os.path.exists(hash_file) and os.path.exists(cache_file):
      with open(hash_file, 'r') as hfd:
        if mtime == int(hfd.readline().strip()) or\
          hash_ == hfd.readline().strip():
          with gzip.open(cache_file, 'rb') as cfd, open(self.drt, 'wb') as tfd:
            tfd.write(cfd.read())
          return (True, self.src, self.drt)
    re = self._run()
    if re[0]:
      with open(self.drt, 'rb') as tfd, gzip.open(cache_file, 'wb') as cfd:
        cfd.write(tfd.read())
      with open(hash_file, 'w') as hfd:
        hfd.write(str(mtime)); hfd.write('\n')
        hfd.write(hash_); hfd.write('\n')
    return re

  @property
  def src(self):
    return self._src

  @property
  def drt(self):
    return self._drt

class EncodeNbtTask(EncodeTask):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  def _run(self):
    try:
      jobj = NBT.nbtToJson(NBT.NBTFile(filename=self.src))
      with open(self.drt, 'w') as fd:
        fd.write(jobj)
      return True, self.src, self.drt
    except:
      print('Encode nbt error:', traceback.format_exc())
      return False, self.src, self._drt

  @property
  def drt(self):
    return self._drt + '.jnbt'

class EncodeRegTask(EncodeTask):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  def _run(self):
    try:
      reg_file = NBT.RegionFile(filename=self.src)
      with open(self.drt, 'w') as fd:
        chunks = NBT.TAG_Compound()
        for m in reg_file.get_metadata():
          chunk = reg_file.get_nbt(m.x, m.z)
          chunk.name = hex(m.x + m.z * 32)[2:]
          chunks.tags.append(chunk)
        fd.write(NBT.nbtToJson(chunks))
      return True, self.src, self.drt
    except:
      print('Encode reg error:', traceback.format_exc())
      return False, self.src, self._drt

  @property
  def drt(self):
    return self._drt + '.jreg'
