
import os
import sys
import io
import shutil
import subprocess
import traceback
import functools
import json
import time
import nbt as NBT
from threading import Timer
import mcdreforged.api.all as MCDR

PLUGIN_METADATA = {
  'id': 'git_backup',
  'version': '1.2.2',
  'name': 'GitBackUp',
  'description': 'Minecraft Git Backup Plugin',
  'author': 'zyxgad',
  'link': 'https://github.com/zyxgad/gitbackup_mcdr',
  'dependencies': {
    'mcdreforged': '>=1.0.0'
  }
}

default_config = {
  'debug': False,
  'git_path': 'git',
  'git_config': {
    'use_remote': False,
    'remote': 'https://github.com/user/repo.git',
    'remote_name': 'backup',
    'branch_name': 'backup',
    'is_setup': False,
    'user_email': 'you@example.com',
    'user_name': 'Your Name'
  },
  'backup_interval': 60 * 60 * 24, # 1 day
  'last_backup_time': 0,
  'push_interval': 60 * 60 * 24, # 1 day
  'last_push_time': 0,
  'back_wait_time': 30,
  'backup_path': './git_backup',
  'cache_path': './git_backup_cache',
  'server_path': './server',
  'need_backup': [
    'world',
  ],
  'ignores': [
    'session.lock',
  ],
  # 0:guest 1:user 2:helper 3:admin
  'minimum_permission_level': {
    'help':    0,
    'status':  1,
    'list':    1,
    'make':    1,
    'back':    2,
    'confirm': 1,
    'abort':   1,
    'reload':  2,
    'save':    2,
    'push':    2,
    'pull':    2
  },
}
config = default_config.copy()
CONFIG_FILE = os.path.join('config', 'GitBackUp.json')
Prefix = '!!gbk'
HelpMessage = '''
------------ {1} v{2} ------------
{0} help 显示帮助信息
{0} status 显示备份状态
{0} list [<limit>] 列出所有/<limit>条备份
{0} make [<comment>] 创建新备份
{0} back [:<index>|<hash id>] 恢复到指定id
{0} confirm <id> 确认回档
{0} abort 取消回档
{0} reload 重新加载配置文件
{0} save 保存配置文件
{0} push 将备份信息推送到远程服务器
{0} pull 拉取远程服务器的备份信息
============ {1} v{2} ============
'''.strip().format(Prefix, PLUGIN_METADATA['name'], PLUGIN_METADATA['version'])

SERVER_OBJ = None

game_saved_callback = None
what_is_doing = None
confirm_callback = None
abort_callback = None

backup_timer = None
need_backup = False

def check_doing(source: MCDR.CommandSource):
  global what_is_doing
  if what_is_doing is None:
    return True
  send_message(source, f'Error: is {what_is_doing} now')
  return False

def change_doing(source: MCDR.CommandSource, sth: str):
  global what_is_doing
  if what_is_doing is None:
    what_is_doing = sth
    return True
  send_message(source, f'Error: is {what_is_doing} now')
  return False

def clear_doing():
  global what_is_doing
  what_is_doing = None

def new_doing(sth: str):
  def _(call):
    @functools.wraps(call)
    def warp_call(source: MCDR.CommandSource, *args, **kwargs):
      if not change_doing(sth):
        return None
      try:
        return call(source, *args, **kwargs)
      finally:
        clear_doing()
    return warp_call
  return _

def debug_message(*args, **kwargs):
  if config['debug']:
    print(*args, **kwargs)

def send_message(source: MCDR.CommandSource or None, *args, sep=' ', prefix='[GBU] '):
  if source is None:
    return
  source.reply(MCDR.RTextList(prefix, args[0], *([MCDR.RTextList(sep, a) for a in args][1:])))

def parse_backup_info(line: str):
  bkid, cmn = line.split(' ', 1)
  a = cmn.split('=', 1)
  date, common = a if len(a) == 2 else (a[0], '')
  return bkid, date, common

def get_backup_info(bid: str or int):
  ecode, out = run_git_cmd('log', '--pretty=oneline', '--no-decorate', '-{}'.format(bid) if isinstance(bid, int) else '')
  if ecode != 0:
    raise RuntimeError('Get log error: ({0}){1}'.format(ecode, out))
  lines = out.splitlines()

  if isinstance(bid, int):
    if len(lines) < bid:
      raise ValueError('Index({}) is out of range'.format(bid))
    return parse_backup_info(lines[bid - 1])
  if isinstance(bid, str):
    i = 0
    while i < len(lines):
      l = lines[i]
      if l.startswith(bid):
        return parse_backup_info(l)
      i += 1
    raise ValueError('Can not found commit by hash "{}"'.format(bid))
  raise TypeError('bid must be "int" or "str"')

def timerCall():
  global backup_timer, need_backup
  backup_timer = None
  if not need_backup:
    return
  SERVER_OBJ.broadcast('[GBU] Auto backuping...')
  command_make_backup(MCDR.PluginCommandSource(SERVER_OBJ), 'Auto backup')

def flushTimer():
  global backup_timer
  if backup_timer is not None:
    backup_timer.cancel()
    backup_timer = None
  global need_backup
  if need_backup:
    bkinterval = config['backup_interval'] - (time.time() - config['last_backup_time'])
    if bkinterval <= 0:
      timerCall()
      return
    backup_timer = Timer(bkinterval, timerCall)
    backup_timer.start()

def flushTimer0():
  global need_backup
  config['last_backup_time'] = time.time()
  if need_backup:
    SERVER_OBJ.broadcast(
      '[GBU] Flush backup timer\n[GBU] the next backup will make after {:.1f} sec'.format(float(config['backup_interval'])))
    flushTimer()

######## something packer ########

def format_command(command: str, text=None, color=MCDR.RColor.yellow, styles=MCDR.RStyle.underlined):
  if text is None: text = command
  return MCDR.RText(text, color=color, styles=styles).c(MCDR.RAction.run_command, command)

def format_git_list(source: MCDR.CommandSource, string, bkid, date, common):
  return (format_command('{0} back {1}'.format(Prefix, bkid), string, color=MCDR.RColor.blue).
    h(f'hash: {bkid}\n', f'date: {date}\n', f'common: {common}\n', '点击回档') if source.is_player else
    MCDR.RText(string + f'  hash: {bkid}\n  date: {date}\n  common: {common}\n', color=MCDR.RColor.blue))

######## COMMANDS ########

def command_help(source: MCDR.CommandSource):
  send_message(source, HelpMessage, prefix='')

def command_status(source: MCDR.CommandSource):
  tmnow = time.time()
  bkid, date, common = get_backup_info(1)
  msg = MCDR.RTextList('------------ git backups ------------\n',
'当前时间: {}\n'.format(get_format_time(tmnow)), format_git_list(source, '最近一次备份: {}\n'.format(bkid[:16]), bkid, date, common),
'下次备份将在{0:.1f}秒后进行\n\
下次推送将在{1:.1f}秒后进行\n------------ git backups ------------'.format(
    float(max(config['backup_interval'] - (tmnow - config['last_backup_time']), 0)),
    float(max(config['push_interval'] - (tmnow - config['last_push_time']), 0)))
  )
  send_message(source, msg, prefix='')

@MCDR.new_thread('GBU')
def command_list_backup(source: MCDR.CommandSource, limit: int or None = None):
  ecode, out = run_git_cmd('log', '--pretty=oneline', '--no-decorate', '' if limit is None else '-{}'.format(limit))
  if ecode != 0:
    send_message(source, out)
    return
  lines = out.splitlines()
  bkid, date, common = parse_backup_info(lines[0])
  latest = format_git_list(source, '{}\n'.format(bkid[:16]), bkid, date, common)
  msg = MCDR.RText('------------ git backups ------------\n')
  debug_message('whiling lines', len(lines))
  i = 0
  while i < len(lines):
    debug_message('parsing line:', i, ':', lines[i])
    bkid, date, common = parse_backup_info(lines[i])
    debug_message('RTextList appending')
    msg = MCDR.RTextList(msg, format_git_list(source, '{0}: {1}: {2}\n'.format(i + 1, bkid[:16], common), bkid, date, common))
    i += 1
  msg = MCDR.RTextList(msg,
    '共{}条备份, 最近一次备份为: '.format(i), latest,
    '------------ git backups ------------')
  send_message(source, msg, prefix='')

@new_doing('making backup')
def command_make_backup(source: MCDR.CommandSource, common: str or None = None):
  common = ('"{date}"' if common is None else '"{date}={common}"').format(date=get_format_time(), common=common)

  @MCDR.new_thread('GBU')
  @new_doing('making backup')
  def call():
    send_message(source, 'Making backup {}...'.format(common))
    _make_backup_files()
    send_message(source, 'Commiting backup {}...'.format(common))
    run_git_cmd('add', '--all')
    ecode, out = run_git_cmd('commit', '-m', common)
    if ecode != 0:
      send_message(source, 'Make backup {0} ERROR:\n{1}'.format(common, out))
      flushTimer0()
      return

    send_message(source, 'Make backup {} SUCCESS'.format(common))
    flushTimer0()

    if config['git_config']['use_remote'] and config['push_interval'] > 0 and \
      config['last_push_time'] + config['push_interval'] < time.time():
      send_message(source, 'pushing now...')
      _command_push_backup(source)

  global game_saved_callback
  game_saved_callback = lambda: (clear_doing(), call())

  send_message(source, "Saving the game")
  source.get_server().execute('save-all flush')

def command_back_backup(source: MCDR.CommandSource, bid):
  if not check_doing(source): return

  bkid, date, common = get_backup_info(int(bid[1:]) if isinstance(bid, str) and bid[0] == ':' else bid)

  @MCDR.new_thread('GBU')
  @new_doing('backing')
  def call(source: MCDR.CommandSource):
    server = source.get_server()

    abort = [False]
    t = config['back_wait_time']
    def call0(source: MCDR.CommandSource):
      if t == -1:
        send_message(source, '已经开始回档, 无法取消')
        return
      send_message(source, '取消回档中')
      abort[0] = True
    global abort_callback
    abort_callback = call0

    while t > 0:
      server.broadcast(MCDR.RTextList('[GBU] {t} 秒后将重启回档至{date}({common})\n[GBU] 输入`'.format(
        Prefix, t=t, date=date, common=common), format_command('{0} abort'.format(Prefix)), '`撤销回档'))
      time.sleep(1)
      if abort[0]:
        server.broadcast('[GBU] 已取消回档')
        return
      t -= 1

    server.stop()
    server.logger.info('[GBU] Wait for server to stop')
    server.wait_for_start()

    if abort[0]:
      server.logger.info('[GBU] 已取消回档')
      server.start()
      return
    t = -1

    if abort_callback is call0:
      abort_callback = None

    server.logger.info('[GBU] Backup now')
    ecode, out = run_git_cmd('reset', '--hard', bkid)
    if ecode == 0:
      if os.path.exists(config['cache_path']): rmfile(config['cache_path'])
      copydir(config['server_path'], config['cache_path'], walk=file_walker if config['debug'] else None)
      for file in config['need_backup']:
        if file == '.gitignore': continue
        tg = os.path.join(config['server_path'], file)
        sc = os.path.join(config['backup_path'], file)
        if os.path.exists(tg): rmfile(tg)
        if os.path.exists(sc): copyto(sc, tg, trycopyfunc=copyfilemc, call_walk=file_walker)
      server.logger.info('[GBU] Backup to {date}({common}) SUCCESS'.format(date=date, common=common))
    else:
      server.logger.info('[GBU] Backup to {date}({common}) ERROR:\n{err}'.format(date=date, common=common, err=out))
    server.logger.info('[GBU] Starting server')
    server.start()

  def call2(source: MCDR.CommandSource):
    global confirm_callback
    source.get_server().broadcast('[GBU] 已取消准备回档')
    if confirm_callback is call:
      confirm_callback = None

  send_message(source, MCDR.RTextList('输入`', format_command('{0} confirm'.format(Prefix)),
    '`确认回档至{date}({common}) `'.format(Prefix, date=date, common=common),
    format_command('{0} abort'.format(Prefix)), '`撤销回档'))
  global confirm_callback, abort_callback
  confirm_callback, abort_callback = call, call2

def command_confirm(source: MCDR.CommandSource):
  global confirm_callback
  if confirm_callback is None:
    send_message(source, '没有正在进行的事件')
  else:
    confirm_callback(source)
    confirm_callback = None

def command_abort(source: MCDR.CommandSource):
  global abort_callback
  if abort_callback is None:
    send_message(source, '没有正在进行的事件')
  else:
    abort_callback(source)
    abort_callback = None

@MCDR.new_thread('GBU')
def command_push_backup(source: MCDR.CommandSource):
  _command_push_backup(source)

@new_doing('pushing')
def _command_push_backup(source: MCDR.CommandSource):
  if not config['git_config']['use_remote']:
    send_message(source, 'Not allowed remote')
    return

  send_message(source, 'Pushing backups')
  ecode, out = run_git_cmd('push', '-f', '-q')
  if ecode != 0:
    send_message(source, 'Push error:\n' + out)
    return

  config['last_push_time'] = time.time()
  send_message(source, 'Push SUCCESS:\n' + out)

def command_pull_backup(source: MCDR.CommandSource):
  send_message(source, '功能开发中')

######## APIs ########

def on_load(server :MCDR.ServerInterface, prev_module):
  global need_backup, SERVER_OBJ
  SERVER_OBJ = server

  load_config(server)
  need_backup = config['backup_interval'] > 0
  clear_doing()
  if prev_module is None:
    server.logger.info('GitBackUp is on load')
  else:
    server.logger.info('GitBackUp is on reload')
    if need_backup and server.is_server_startup():
      flushTimer()

  setup_git(server)
  register_command(server)

def on_unload(server: MCDR.ServerInterface):
  global need_backup
  server.logger.info('GitBackUp is on unload')
  need_backup = False
  flushTimer()

  global SERVER_OBJ
  SERVER_OBJ = None

def on_remove(server: MCDR.ServerInterface):
  global need_backup
  server.logger.info('GitBackUp is on disable')
  need_backup = False
  flushTimer()
  save_config(server)

  global SERVER_OBJ
  SERVER_OBJ = None

def on_server_startup(server: MCDR.ServerInterface):
  server.logger.info('[GBU] server is startup')
  if need_backup:
    flushTimer()

def on_mcdr_stop(server: MCDR.ServerInterface):
  server.logger.info('[GBU] mcdr is stop')
  save_config(server)

def on_info(server: MCDR.ServerInterface, info: MCDR.Info):
  if not info.is_user:
    if info.content == 'Saved the game' or info.content == 'Saved the world':
      global game_saved_callback
      if game_saved_callback is not None:
        game_saved_callback()
        game_saved_callback = None

@MCDR.new_thread('GBU')
def setup_git(server: MCDR.ServerInterface):
  # check git
  ecode, out = run_sh_cmd('{git} --version'.format(git=config['git_path']))
  if ecode != 0:
    raise RuntimeError('Can not found git at "{}"'.format(config['git_path']))
  server.logger.info(out)

  if not os.path.isdir(config['backup_path']): os.makedirs(config['backup_path'])

  def _run_git_cmd_hp(child, *args):
    ecode, out = run_git_cmd(child, *args)
    server.logger.info(out)
    if ecode != 0:
      raise RuntimeError('Init git error({0}): {1}'.format(ecode, out))
  
  if not os.path.isdir(os.path.join(config['backup_path'], '.git')):
    config['git_config']['is_setup'] = False
    # init git
    server.logger.info('git is initing')
    _run_git_cmd_hp('init')
    _run_git_cmd_hp('config', 'user.email', '"{}"'.format(config['git_config']['user_email']))
    _run_git_cmd_hp('config', 'user.name', '"{}"'.format(config['git_config']['user_name']))
    _run_git_cmd_hp('checkout', '-b', config['git_config']['branch_name'])
    _run_git_cmd_hp('config', 'credential.helper', 'store')
    _run_git_cmd_hp('config', 'core.autocrlf', 'false')
    if config['git_config']['use_remote']:
      _run_git_cmd_hp('remote', 'add', config['git_config']['remote_name'], config['git_config']['remote'])
      try:
        _run_git_cmd_hp('pull', '--set-upstream', config['git_config']['remote_name'], config['git_config']['branch_name'])
      except:
        pass
  else:
    _run_git_cmd_hp('config', 'user.email', '"{}"'.format(config['git_config']['user_email']))
    _run_git_cmd_hp('config', 'user.name', '"{}"'.format(config['git_config']['user_name']))
  server.logger.info('git email: ' + run_git_cmd('config', 'user.email')[1])
  server.logger.info('git user: ' + run_git_cmd('config', 'user.name')[1])

  if config['git_config']['use_remote']:
    ecode, out = run_git_cmd('remote', 'get-url', config['git_config']['remote_name'])
    if ecode != 0 or out.strip() != config['git_config']['remote']:
      server.logger.info('new url: ' + config['git_config']['remote'])
      _run_git_cmd_hp('remote', 'set-url', config['git_config']['remote_name'], config['git_config']['remote'])

  with open(os.path.join(config['backup_path'], '.gitignore'), 'w') as fd:
    fd.write('# Make by GitBackUp at {}\n'.format(get_format_time()))
    fd.writelines(config['ignores'])

  if config['git_config']['use_remote']:
    server.logger.info('git remote: {}'.format(config['git_config']['remote']))
  if not config['git_config']['is_setup']:
    _run_git_cmd_hp('add', '--all')
    _run_git_cmd_hp('commit', '-m', '"{}=Setup commit"'.format(get_format_time()))
    if config['git_config']['use_remote']:
      proc = subprocess.Popen(
        '{git} -C {path} push -u {remote_name} {branch}'.format(
          git=config['git_path'], path=config['backup_path'],
          branch=config['git_config']['branch_name'], remote_name=config['git_config']['remote_name']),
        shell=True,
        stdout=sys.stdout, stderr=sys.stdout, stdin=sys.stdin,
        bufsize=-1)
      ecode = proc.wait()
      if ecode is not None and ecode != 0:
        raise RuntimeError('first push error')
    config['git_config']['is_setup'] = True

def register_command(server: MCDR.ServerInterface):
  def get_literal_node(literal):
    lvl = config['minimum_permission_level'].get(literal, 0)
    return MCDR.Literal(literal).requires(lambda src: src.has_permission(lvl), lambda: '权限不足')
  server.register_command(
    get_literal_node(Prefix).
    runs(command_help).
    then(get_literal_node('help').runs(command_help)).
    then(get_literal_node('status').runs(command_status)).
    then(get_literal_node('list').
      runs(lambda src: command_list_backup(src, None)).
      then(MCDR.Integer('limit').runs(lambda src, ctx: command_list_backup(src, ctx['limit'])))
    ).
    then(get_literal_node('make').
      runs(lambda src: command_make_backup(src, None)).
      then(MCDR.GreedyText('common').runs(lambda src, ctx: command_make_backup(src, ctx['common'])))
    ).
    then(get_literal_node('back').
      runs(lambda src: command_back_backup(src, ':1')).
      then(MCDR.Text('id').runs(lambda src, ctx: command_back_backup(src, ctx['id'])))).
    then(get_literal_node('confirm').runs(command_confirm)).
    then(get_literal_node('abort').runs(command_abort)).
    then(get_literal_node('reload').runs(lambda src: load_config(server, src))).
    then(get_literal_node('save').runs(lambda src: save_config(server, src))).
    then(get_literal_node('push').runs(lambda src: command_push_backup(src))).
    then(get_literal_node('pull').runs(lambda src: command_pull_backup(src)))
  )

def load_config(server: MCDR.ServerInterface, source: MCDR.CommandSource or None = None):
  global config
  try:
    config = {}
    with open(CONFIG_FILE) as file:
      js = json.load(file)
    for key in default_config.keys():
      config[key] = (js if key in js else default_config)[key]
    server.logger.info('Config file loaded')
    send_message(source, '配置文件加载成功')
  except:
    server.logger.info('Fail to read config file, using default value')
    send_message(source, '配置文件加载失败, 切换默认配置')
    config = default_config.copy()
    save_config(server, source)

def save_config(server: MCDR.ServerInterface, source: MCDR.CommandSource or None = None):
  with open(CONFIG_FILE, 'w') as file:
    json.dump(config, file, indent=4)
    server.logger.info('Config file saved')
    send_message(source, '配置文件保存成功')

################## HELPER ##################

def dir_walker(size):
  current = 0
  while True:
    f = yield
    current += 1
    SERVER_OBJ.logger.info('file "{f}" {c}/{a}({c_a})', f=f, c=current, a=size, c_a=int(current / size * 100))
    if current >= size:
      break
  yield
  return

def file_walker(size):
  if isinstance(size, int):
    return dir_walker(size)
  else:
    SERVER_OBJ.logger.info('file "{f}"', f=f)

def _make_backup_files():
  for file in config['need_backup']:
    sc = os.path.join(config['server_path'], file)
    tg = os.path.join(config['backup_path'], file)
    if os.path.exists(tg): rmfile(tg)
    if os.path.exists(sc): copyto(sc, tg, trycopyfunc=copymcfile, call_walk=file_walker)

def copymcfile(src, drt):
  if os.path.isfile(src):
    try:
      if src.endswith(('.dat', '.dat_old')):
        debug_message('try change "{0}" to "{1}"'.format(src, drt))
        jobj = NBT.nbtToJson(NBT.NBTFile(filename=src))
        with open('{}.jnbt'.format(drt), 'w') as fd:
          fd.write(jobj)
        debug_message('change "{0}" to "{1}"'.format(src, drt))
        return True
      if src.endswith('.mca'):
        debug_message('try change "{0}"(mca)'.format(src))
        reg_file = NBT.RegionFile(filename=src)
        x, z = 0, 0
        while z < 32:
          try:
            jobj = NBT.nbtToJson(reg_file.get_chunk(x, z))
            with open('{drt}.{x_z}.jreg'.format(drt=drt, x_z=z * 32 + x), 'w') as fd:
              fd.write(jobj)
            debug_message('change "{0}" to "{1}"({x}, {z})'.format(src,
              '{drt}.{x_z}.jreg'.format(drt=drt, x_z=z * 32 + x), x=x, z=z))
          except:
              pass
          x += 1
          if x >= 32:
            x = 0
            z += 1
        return True
    except KeyboardInterrupt:
      raise
    except BaseException as e:
      debug_message('err: ', e)
  return False

def copyfilemc(src, drt):
  if os.path.isfile(src):
    try:
      if src.endswith('.jnbt'):
        debug_message('try write "{0}" to "{1}"'.format(src, drt[:-5]))
        with open(src, 'r') as fd:
          nbt_ = NBT.jsonToNbt(fd.read())
          nbt_.write_file(filename=drt[:-5])
        debug_message('write "{0}" to "{1}"'.format(src, drt[:-5]))
        return True
      if src.endswith('.jreg'):
        debug_message('try write "{0}"(mca)'.format(src))
        drt0, x_z = drt[:-5].rsplit('.', 1)
        x_z = int(x_z)
        x, z = x_z % 32, x_z // 32
        with open(src, 'r') as fd, open(drt0, 'r+b' if os.path.exists(drt0) else 'w+b') as reg_fd:
          reg_file = NBT.RegionFile(fileobj=reg_fd)
          reg_file.write_chunk(x, z, NBT.jsonToNbt(fd.read()))
        debug_message('write "{0}"({x}, {z}) to "{1}"'.format(src, drt0, x=x, z=z))
        return True
    except KeyboardInterrupt:
      raise
    except BaseException as e:
      debug_message('err: ', traceback.format_exc())
  return False

################## UTILS ##################

def get_format_time(time_=None):
  return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_ if time_ is not None else time.time()))

def run_sh_cmd(source: str):
  debug_message('Running command "{}"'.format(source))
  proc = subprocess.Popen(
    source, shell=True,
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=sys.stdin,
    bufsize=-1)
  stdout0 = [b'', False]
  @MCDR.new_thread("GBU-Popen-reader")
  def reader():
    try:
      debug_message('reading stdout...')
      while True:
        buf = proc.stdout.read()
        debug_message('read:', buf)
        if len(buf) == 0:
          break
        stdout0[0] += buf
      debug_message('end read')
    finally:
      stdout0[1] = True
  reader()
  debug_message('waiting command...')
  exitid = proc.wait()
  debug_message('decoding stdout...')
  while not stdout0[1]:
    time.sleep(0.05)
  stdout = ''
  if len(stdout0[0]) > 0:
    try:
      stdout = stdout0[0].decode('utf-8')
    except UnicodeDecodeError:
      stdout = stdout0[0].decode('gbk')
  debug_message('returning...')
  return 0 if exitid is None else exitid, stdout

def run_git_cmd(child: str, *args):
  command = '{git} -C {path} --no-pager {child} {args}'.format(
    git=config['git_path'], path=config['backup_path'], child=child, args=' '.join(args))
  return run_sh_cmd(command)

def rmfile(tg):
  debug_message('Removing "{}"'.format(tg))
  if os.path.isdir(tg):
    shutil.rmtree(tg)
  elif os.path.isfile(tg):
    os.remove(tg)

def copydir(src, drt, trycopyfunc=None, walk=None):
  debug_message('Copying dir "{0}" to "{1}"'.format(src, drt))
  if not os.path.exists(drt): os.makedirs(drt)
  prefilelist = []
  for root, dirs, files in os.walk(src):
    droot = os.path.join(drt, os.path.relpath(root, src))
    for d in dirs:
      if d in config['ignores']: continue
      d0 = os.path.join(droot, d)
      if not os.path.exists(d0): os.mkdir(d0)
    for f in files:
      if f in config['ignores']: continue
      prefilelist.append((os.path.join(root, f), os.path.join(droot, f)))

  if walk is not None:
    walker = walk(len(prefilelist))
    walker.send(None)
  for f in prefilelist:
    copyfile(f[0], f[1], trycopyfunc=trycopyfunc, successcall=(lambda f: walker.send(f)) if walk is not None else None)

def copyfile(src, drt, trycopyfunc=None, successcall=None):
  debug_message('Copying file "{0}" to "{1}"'.format(src, drt))
  if os.path.basename(src) in config['ignores']:
    return
  
  if trycopyfunc is None or not trycopyfunc(src, drt):
    shutil.copy(src, drt)
  successcall is not None and successcall(src)

def copyto(src, drt, trycopyfunc=None, call_walk=None):
  debug_message('Copying "{0}" to "{1}"'.format(src, drt))
  if os.path.basename(src) in config['ignores']:
    return
  if os.path.isdir(src):
    copydir(src, drt, trycopyfunc=trycopyfunc, walk=call_walk)
  elif os.path.isfile(src):
    copyfile(src, drt, trycopyfunc=trycopyfunc, successcall=call_walk)
