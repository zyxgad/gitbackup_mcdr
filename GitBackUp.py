
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
from multiprocessing import Queue

import taskhelper

import mcdreforged.api.all as MCDR

PLUGIN_METADATA = {
  'id': 'git_backup',
  'version': '1.2.6',
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
  'helper_count': 1,
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
MSG_ID = MCDR.RText('[GBU]', color=MCDR.RColor.green)
HelpMessage = '''
------------ {1} v{2} ------------
{0} help 显示帮助信息
{0} status 显示备份状态
{0} list [<limit>] 列出所有/<limit>条备份
{0} make [<comment>] 创建新备份
{0} back [:<index>|<hash id>] 恢复到指定id
{0} confirm 确认回档
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
confirm_callback = {}
abort_callback = {}

backup_timer = None
need_backup = False

helper_manager = None

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

def try_doing(call):
  @functools.wraps(call)
  def warp_call(source: MCDR.CommandSource=None, *args, **kwargs):
    if not check_doing(source):
      return None
    return call(source, *args, **kwargs)
  return warp_call

def new_doing(sth: str):
  def _(call):
    @functools.wraps(call)
    def warp_call(source: MCDR.CommandSource=None, *args, **kwargs):
      if not change_doing(source, sth):
        return None
      try:
        return call(source, *args, **kwargs)
      finally:
        clear_doing()
    return warp_call
  return _

def set_confirm_callback(source: MCDR.CommandSource or None, call):
  global confirm_callback
  confirm_callback[None if source is None else (source.player if source.is_player else '')] = call

def cancel_confirm_callback(source: MCDR.CommandSource or None, call=None):
  global confirm_callback
  sname = None if source is None else (source.player if source.is_player else '')
  if call is not None and confirm_callback.get(sname, None) is not call:
    return False
  return confirm_callback.pop(sname, None) is not None

def set_abort_callback(source: MCDR.CommandSource or None, call):
  global abort_callback
  abort_callback[None if source is None else (source.player if source.is_player else '')] = call

def cancel_abort_callback(source: MCDR.CommandSource or None, call=None):
  global abort_callback
  sname = None if source is None else (source.player if source.is_player else '')
  if call is not None and abort_callback.get(sname, None) is not call:
    return False
  return abort_callback.pop(sname, None) is not None

def debug_message(*args, **kwargs):
  if config['debug']:
      print('[GBU-DEBUG]', *args, **kwargs)

def send_message(source: MCDR.CommandSource, *args, sep=' ', prefix=MSG_ID):
  if source is not None:
    source.reply(MCDR.RTextList(prefix, args[0], *([MCDR.RTextList(sep, a) for a in args][1:])))

def broadcast_message(*args, sep=' ', prefix=MSG_ID):
  if SERVER_OBJ is not None:
    SERVER_OBJ.broadcast(MCDR.RTextList(prefix, args[0], *([MCDR.RTextList(sep, a) for a in args][1:])))

def log_info(*args, sep=' ', prefix=MSG_ID):
  if SERVER_OBJ is not None:
    SERVER_OBJ.logger.info(MCDR.RTextList(prefix, args[0], *([MCDR.RTextList(sep, a) for a in args][1:])))

def parse_backup_info(line: str):
  bkid, cmn = line.split(' ', 1)
  a = cmn.split('=', 1)
  date, common = a if len(a) == 2 else (a[0], '')
  return bkid, date, common

def get_backup_info(bid: str or int):
  use_hash = True
  bid_ = bid
  if isinstance(bid, int):
    use_hash = False
    bid_ = '-{}'.format(bid_)
  elif not isinstance(bid, str):
    raise TypeError('bid must be "int" or "str"')
  ecode, out = run_git_cmd('log', '--pretty=oneline', '--no-decorate', bid_, '--')
  if ecode != 0:
    raise RuntimeError('Get log error: ({0}){1}'.format(ecode, out))
  lines = out.splitlines()

  if use_hash:
    if len(lines) == 0:
      raise ValueError('Can not found commit by hash "{}"'.format(bid))
    return parse_backup_info(lines[0])
  if len(lines) < bid:
    raise ValueError('Index {} is out of range'.format(bid))
  return parse_backup_info(lines[bid - 1])

def timerCall():
  global backup_timer, need_backup
  backup_timer = None
  if not need_backup:
    return
  broadcast_message('Auto backuping...')
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
    broadcast_message('Flush backup timer\n', 
      MSG_ID, 'the next backup will make after {intv:.1f}sec'.
      format(intv=float(config['backup_interval'])), sep='')
    flushTimer()

######## something packer ########

def format_command(command: str, text=None, color=MCDR.RColor.yellow, styles=MCDR.RStyle.underlined):
  if text is None: text = command
  return MCDR.RText(text, color=color, styles=styles).c(MCDR.RAction.run_command, command)

def format_git_list(source: MCDR.CommandSource, string, bkid, date, common):
  return (format_command('{0} back {1}'.format(Prefix, bkid), string + '\n', color=MCDR.RColor.blue).
    h(f'hash: {bkid}\n', f'date: {date}\n', f'common: {common}\n', '点击回档') if source.is_player else
    MCDR.RText(f'{string}\n  hash: {bkid}\n  date: {date}\n  common: {common}\n', color=MCDR.RColor.blue))

######## COMMANDS ########

def command_help(source: MCDR.CommandSource):
  send_message(source, HelpMessage, prefix='')

@MCDR.new_thread('GBU')
def command_status(source: MCDR.CommandSource):
  dir_size = get_size(config['backup_path']) / (1024 * 1024)
  dir_true_size = get_size(os.path.join(config['backup_path'], '.git')) / (1024 * 1024)
  tmnow = time.time()
  bkid, date, common = get_backup_info(1)
  send_message(source, MCDR.RTextList('------------ git backups ------------\n',
'''版本: v{version}
当前时间: {tmnow}
正在进行的事件: {doing}
最近一次备份: '''.format(
    version=PLUGIN_METADATA['version'],
    tmnow=get_format_time(tmnow), doing=what_is_doing),
    format_git_list(source, bkid[:16], bkid, date, common),
    '''下次备份将在{backup_tout:.1f}秒后进行
下次推送将在{push_tout:.1f}秒后进行
备份文件夹总大小:{dir_size:.2f}MB
  实际大小:{dir_true_size:.2f}MB
  缓存大小:{dir_cache_size:.2f}MB
------------ git backups ------------'''.format(
    backup_tout=max(float(config['backup_interval'] - (tmnow - config['last_backup_time'])), 0.0),
    push_tout=max(float(config['push_interval'] - (tmnow - config['last_push_time'])), 0.0),
    dir_size=dir_size,
    dir_true_size=dir_true_size,
    dir_cache_size=dir_size - dir_true_size
  )), prefix='')

@MCDR.new_thread('GBU')
def command_list_backup(source: MCDR.CommandSource, limit: int or None = None):
  ecode, out = run_git_cmd('log', '--pretty=oneline', '--no-decorate', '' if limit is None else '-{}'.format(limit))
  if ecode != 0:
    send_message(source, out)
    return
  lines = out.splitlines()
  bkid, date, common = parse_backup_info(lines[0])
  latest = format_git_list(source, bkid[:16], bkid, date, common)
  msg = MCDR.RText('------------ git backups ------------\n')
  debug_message('whiling lines', len(lines))
  for i, l in enumerate(lines):
    debug_message('parsing line:', i, ':', l)
    bkid, date, common = parse_backup_info(l)
    msg = MCDR.RTextList(msg, format_git_list(source, '{0}: {1}: {2}'.format(i + 1, bkid[:16], common), bkid, date, common))
  msg = MCDR.RTextList(msg,
    '共{}条备份, 最近一次备份为: '.format(i), latest,
    '------------ git backups ------------')
  send_message(source, msg, prefix='')

@new_doing('pre making backup')
def command_make_backup(source: MCDR.CommandSource, common: str or None = None):
  common = ('"{date}"' if common is None else '"{date}={common}"').format(date=get_format_time(), common=common)

  @MCDR.new_thread('GBU')
  @new_doing('making backup')
  def call(_): # MCDR.CommandSource(None)
    start_time = time.time()
    broadcast_message('Making backup {}...'.format(common))
    _make_backup_files()
    source.get_server().execute('save-on')
    broadcast_message("Enable auto save")
    broadcast_message('Commiting backup {}...'.format(common))
    run_git_cmd('add', '--all')
    ecode, out = run_git_cmd('commit', '-m', common)
    if ecode != 0:
      broadcast_message('Make backup {0} ERROR:\n{1}'.format(common, out))
      flushTimer0()
      return
    use_time = time.time() - start_time
    broadcast_message('Make backup {common} SUCCESS, use time {time:.2f}sec'.format(common=common, time=use_time))
    flushTimer0()

    if config['git_config']['use_remote'] and config['push_interval'] > 0 and \
      config['last_push_time'] + config['push_interval'] < time.time():
      send_message(source, 'There are out {}sec not push, pushing now...'.format(config['push_interval']))
      _command_push_backup(source)

  global game_saved_callback
  game_saved_callback = lambda: (clear_doing(), call())

  broadcast_message("Pre making backup")
  source.get_server().execute('save-off')
  broadcast_message("Disable auto save")
  broadcast_message("Saving the game...")
  source.get_server().execute('save-all flush')

@try_doing
def command_back_backup(source: MCDR.CommandSource, bid):
  try:
    bkid, date, common = get_backup_info(int(bid[1:]) if isinstance(bid, str) and bid[0] == ':' else bid)
  except ValueError as e:
    send_message(source, MCDR.RText(str(e), color=MCDR.RColor.red, styles=MCDR.RStyle.underlined))
    return

  @MCDR.new_thread('GBU')
  @new_doing('backing')
  def call(source: MCDR.CommandSource):
    cancel_abort_callback(source, call2)

    server = source.get_server()

    abort = [False]
    t = config['back_wait_time']
    def call0(source: MCDR.CommandSource):
      if t == -1:
        send_message(source, '已经开始回档, 无法取消')
        return
      send_message(source, '取消回档中')
      abort[0] = True
    set_abort_callback(None, call0)

    while t > 0:
      broadcast_message(MCDR.RTextList('{t} 秒后将重启回档至{date}({common}), 输入`'.format(
        Prefix, t=t, date=date, common=common), format_command('{0} abort'.format(Prefix)), '`撤销回档'))
      time.sleep(1)
      if abort[0]:
        broadcast_message('已取消回档')
        return
      t -= 1

    cancel_abort_callback(None, call)

    server.stop()
    log_info('Wait for server to stop')
    server.wait_for_start()

    if abort[0]:
      log_info('已取消回档')
      server.start()
      return
    t = -1

    log_info('Backup now')
    ecode, out = run_git_cmd('reset', '--hard', bkid)
    if ecode == 0:
      try:
        if os.path.exists(config['cache_path']): rmfile(config['cache_path'])
        copydir(config['server_path'], config['cache_path'], walk=file_walker if config['debug'] else None)
        for file in config['need_backup']:
          if file == '.gitignore': continue
          tg = os.path.join(config['server_path'], file)
          sc = os.path.join(config['backup_path'], file)
          if os.path.exists(tg): rmfile(tg)
          if os.path.exists(sc): copyto(sc, tg, trycopyfunc=copyfilemc, call_walk=file_walker)
        helper_manager.wait_task_all()
        log_info('Backup to {date}({common})'.format(date=date, common=common),
          MCDR.RText('SUCCESS', color=MCDR.RColor.green, styles=MCDR.RStyle.underlined))
      except Exception as e:
        log_info('Backup to {date}({common})'.format(date=date, common=common),
          MCDR.RText('ERROR:\n' + traceback.format_exc(), color=MCDR.RColor.red, styles=MCDR.RStyle.underlined))
    else:
      log_info('Backup to {date}({common})'.format(date=date, common=common),
        MCDR.RText('ERROR:\n' + out, color=MCDR.RColor.red, styles=MCDR.RStyle.underlined))
    log_info('Starting server')
    server.start()

  def call2(source: MCDR.CommandSource):
    cancel_confirm_callback(source, call)
    broadcast_message('已取消准备回档')

  send_message(source, MCDR.RTextList('输入`', format_command(Prefix + ' confirm'),
    '`确认回档至{date}({common}) `'.format(date=date, common=common),
    format_command(Prefix + ' abort'), '`撤销回档'))
  set_confirm_callback(source, call)
  set_abort_callback(source, call2)

def command_confirm(source: MCDR.CommandSource):
  global confirm_callback
  debug_message('confirm_callback:', confirm_callback)
  call = confirm_callback.pop(source.player if source.is_player else '', None)
  if call is None:
    call = confirm_callback.pop(None, None)
    if call is None:
      send_message(source, '没有正在进行的事件')
      return
  call(source)

def command_abort(source: MCDR.CommandSource):
  global abort_callback
  call = abort_callback.pop(source.player if source.is_player else '', None)
  if call is None:
    call = abort_callback.pop(None, None)
    if call is None:
      send_message(source, '没有正在进行的事件')
      return
  call(source)

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
  global need_backup, SERVER_OBJ, helper_manager
  SERVER_OBJ = server

  load_config(server)
  need_backup = config['backup_interval'] > 0
  clear_doing()
  helper_manager = HelperManager(config['helper_count'])

  if prev_module is None:
    log_info('GitBackUp is on load')
  else:
    log_info('GitBackUp is on reload')
    if need_backup and server.is_server_startup():
      flushTimer()

  setup_git(server)
  register_command(server)

def on_unload(server: MCDR.ServerInterface):
  global need_backup
  log_info('GitBackUp is on unload')
  need_backup = False
  flushTimer()

  global SERVER_OBJ, helper_manager
  SERVER_OBJ = None
  if helper_manager is not None:
    helper_manager.clear()
    helper_manager = None

def on_remove(server: MCDR.ServerInterface):
  global need_backup
  log_info('GitBackUp is on disable')
  need_backup = False
  flushTimer()
  save_config(server)

  global SERVER_OBJ, helper_manager
  SERVER_OBJ = None
  if helper_manager is not None:
    helper_manager.clear()
    helper_manager = None

def on_server_startup(server: MCDR.ServerInterface):
  log_info('server is startup')
  if need_backup:
    flushTimer()

def on_server_stop(server: MCDR.ServerInterface, server_return_code: int):
  save_config(server)

def on_mcdr_stop(server: MCDR.ServerInterface):
  log_info('mcdr is stop')
  save_config(server)

def on_info(server: MCDR.ServerInterface, info: MCDR.Info):
  if not info.is_user:
    if info.content == 'Saved the game' or info.content == 'Saved the world':
      global game_saved_callback
      if game_saved_callback is not None:
        game_saved_callback()
        game_saved_callback = None

@MCDR.new_thread('GBU-setup')
def setup_git(server: MCDR.ServerInterface):
  # check git
  ecode, out = run_sh_cmd('{git} --version'.format(git=config['git_path']))
  if ecode != 0:
    raise RuntimeError('Can not found git at "{}"'.format(config['git_path']))
  log_info(out.strip())

  if not os.path.isdir(config['backup_path']): os.makedirs(config['backup_path'])

  def _run_git_cmd_hp(child, *args):
    ecode, out = run_git_cmd(child, *args)
    log_info(out.strip())
    if ecode != 0:
      raise RuntimeError('Init git error({0}): {1}'.format(ecode, out))
  
  if not os.path.isdir(os.path.join(config['backup_path'], '.git')):
    config['git_config']['is_setup'] = False
    # init git
    log_info('git is initing')
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
  log_info('git email: ' + run_git_cmd('config', 'user.email')[1].strip())
  log_info('git user: ' + run_git_cmd('config', 'user.name')[1].strip())

  if config['git_config']['use_remote']:
    ecode, out = run_git_cmd('remote', 'get-url', config['git_config']['remote_name'])
    if ecode != 0 or out.strip() != config['git_config']['remote']:
      log_info('new url: ' + config['git_config']['remote'])
      _run_git_cmd_hp('remote', 'set-url', config['git_config']['remote_name'], config['git_config']['remote'])

  with open(os.path.join(config['backup_path'], '.gitignore'), 'w') as fd:
    fd.write('# Make by GitBackUp at {}\n'.format(get_format_time()))
    fd.write('\n'.join(config['ignores']))
    fd.write('\n*.gbu')

  if config['git_config']['use_remote']:
    log_info('git remote: {}'.format(config['git_config']['remote']))
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
      then(MCDR.Integer('limit').requires(lambda src, ctx: ctx['limit'] > 0, lambda: 'limit 需要大于 0').
        runs(lambda src, ctx: command_list_backup(src, ctx['limit'])))
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
    config = default_config.copy()
    with open(CONFIG_FILE) as file:
      js = json.load(file)
      for key in js.keys():
        config[key] = js[key]
    log_info('Config file loaded')
    send_message(source, '配置文件加载成功')
  except:
    log_info('Fail to read config file, using default value')
    send_message(source, '配置文件加载失败, 切换默认配置')
    config = default_config.copy()
    save_config(server, source)

def save_config(server: MCDR.ServerInterface, source: MCDR.CommandSource or None = None):
  with open(CONFIG_FILE, 'w') as file:
    json.dump(config, file, indent=4)
    log_info('Config file saved')
    send_message(source, '配置文件保存成功')

################## HELPER ##################

class HelperManager:
  def __init__(self, helpers: int=1):
    self._helpers = 0
    self.__helper_list = []
    # self.__helper_list = [[0, taskhelper.HelperProcess(name='gbu-helper-{}'.format(i)), 0] for i in range(1, 1 + self._helpers)]
    self.helpers = helpers
    for h in self.__helper_list:
      h[1].start()

  @property
  def helpers(self):
    return self._helpers

  @helpers.setter
  def helpers(self, helpers):
    assert isinstance(helpers, int) and helpers > 0, "Must be number and large than 0"
    self._helpers = helpers
    for i in range(len(self.__helper_list), self._helpers):
      self.__helper_list.append([0, taskhelper.HelperProcess(name='gbu-helper-{}'.format(i + 1)), 0])

  def add_task(self, task):
    self.__helper_list = sorted(self.__helper_list, key=lambda o: o[2])
    debug_message('helper list:', self.__helper_list)
    h = self.__helper_list[0]
    h[1].run_task(task)
    h[0] += 1
    h[2] += os.stat(task.src).st_size
    debug_message('after add current helper:', hex(id(h[1])), h)

  def wait_task_all(self, call=None):
    if not callable(call): call = lambda _: None
    waits = self.__helper_list.copy()
    def re_one(helper):
      re = helper[1].wait_task()
      helper[0] -= 1
      helper[2] -= os.stat(re[1]).st_size
      debug_message('after wait helper:', hex(id(helper[1])), helper)
      call(re)

    while len(waits) > 0:
      for i, helper in enumerate(waits):
        while not helper[1].task_empty():
          re_one(helper)
        if helper[0] == 0:
          waits.pop(i)
      if len(waits) == 0:
        break
      helper = sorted(waits, key=lambda o: o[2], reverse=True)[0]
      if helper[0] > 0:
        re_one(helper)
      if helper[0] == 0:
        waits.pop(0)

  def clear(self):
    for h in self.__helper_list:
      if h[1].is_alive():
        h[1].exit()
    for h in self.__helper_list:
      if h[1].is_alive():
        h[1].kill()
    self.__helper_list.clear()

  def __del__(self):
    self.clear()

def dir_walker(size):
  current = 0
  while True:
    f = yield
    if not f[0]:
      copyfile(f[1], f[2])
    current += 1
    log_info('file "{f1}"->"{f2}" {c}/{a} ({c_a}%)'.format(f1=f[1], f2=f[2], c=current, a=size, c_a=int(current / size * 100)))
    if current >= size:
      break
  yield
  return

def file_walker(s):
  if isinstance(s, int):
    return dir_walker(s)
  else:
    if not s[0]:
      copyfile(s[1], s[2])
    log_info('file "{f1}"->"{f2}"'.format(f1=s[1], f2=s[2]))

def _make_backup_files():
  for file in config['need_backup']:
    sc = os.path.join(config['server_path'], file)
    tg = os.path.join(config['backup_path'], file)
    if os.path.exists(sc): copyto(sc, tg, trycopyfunc=copymcfile, call_walk=file_walker)

def copymcfile(src, drt):
  if os.path.isfile(src):
    if src.endswith(('.dat', '.dat_old')):
      helper_manager.add_task(taskhelper.EncodeNbtTask(src, drt))
      return True
    if src.endswith(('.mca', '.mcr')):
      helper_manager.add_task(taskhelper.EncodeRegTask(src, drt))
      return True
  debug_message('use native copy', src, drt)
  return False

def copyfilemc(src, drt):
  if os.path.isfile(src):
    if src.endswith('.jnbt'):
      helper_manager.add_task(taskhelper.DecodeNbtTask(src, drt))
      return True
    if src.endswith('.jreg'):
      helper_manager.add_task(taskhelper.DecodeRegTask(src, drt))
      return True
  debug_message('use native copy', src, drt)
  return False

################## UTILS ##################

def get_format_time(time_=None):
  return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_ if time_ is not None else time.time()))

def run_sh_cmd(source: str):
  debug_message('Running command "{}"'.format(source))
  proc = subprocess.Popen(
    source, shell=True,
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
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

def get_size(path: str):
  if os.path.isfile(path):
    return os.stat(path).st_size
  elif os.path.isdir(path):
    size = 0
    for root, _, files in os.walk(path):
      for f in files:
        file = os.path.join(root, f)
        if os.path.isfile(file):
          size += os.stat(file).st_size
    return size
  return 0

def rmfile(tg: str):
  debug_message('Removing "{}"'.format(tg))
  if os.path.isdir(tg):
    shutil.rmtree(tg)
  elif os.path.isfile(tg):
    os.remove(tg)

def copydir(src: str, drt: str, trycopyfunc=None, walk=None):
  debug_message('Copying dir "{0}" to "{1}"'.format(src, drt))
  if not os.path.exists(drt): os.makedirs(drt)
  filelist = []
  for root, dirs, files in os.walk(src):
    droot = os.path.join(drt, os.path.relpath(root, src))
    for d in dirs:
      if d in config['ignores']: continue
      d0 = os.path.join(droot, d)
      if os.path.exists(d0):
        for f in os.listdir(d0):
          file = os.path.join(d0, f)
          if os.path.isfile(file) and not f.endswith('.gbu'):
            os.remove(file)
      else:
        os.mkdir(d0)
    for f in files:
      if f in config['ignores'] or f.endswith('.gbu'): continue
      filelist.append((os.path.join(root, f), os.path.join(droot, f)))

  debug_message('filelist:', filelist)
  successcall = lambda _: None
  if callable(walk):
    walker = walk(len(filelist))
    walker.send(None)
    successcall = walker.send
  for f in filelist:
    debug_message('Copying file "{0}" to "{1}"'.format(f[0], f[1]))
    if not (callable(trycopyfunc) and trycopyfunc(f[0], f[1])):
      shutil.copy(f[0], f[1])
      successcall((True, f[0], f[1]))
  helper_manager.wait_task_all(successcall)

def copyfile(src: str, drt: str, trycopyfunc=None, successcall=None):
  debug_message('Copying file "{0}" to "{1}"'.format(src, drt))
  if os.path.basename(src) in config['ignores'] or src.endswith('.gbu'):
    return

  if not (callable(trycopyfunc) and trycopyfunc(src, drt)):
    shutil.copy(src, drt)
  if callable(successcall): successcall((True, src, drt))

def copyto(src: str, drt: str, trycopyfunc=None, call_walk=None):
  debug_message('Copying "{0}" to "{1}"'.format(src, drt))
  if os.path.basename(src) in config['ignores']:
    return
  if os.path.isdir(src):
    copydir(src, drt, trycopyfunc=trycopyfunc, walk=call_walk)
  elif os.path.isfile(src):
    copyfile(src, drt, trycopyfunc=trycopyfunc, successcall=call_walk)
