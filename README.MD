
# 中文

## 依赖
- **mcdreforged** >= ***1.0.0***

## 功能
- 支持本地保存备份
- 支持与远端服务器同步备份, 防止备份丢失
- 支持定时备份与推送
- ~~白嫖github专用~~

## 配置
### 提前准备
1. ssh-client
2. git
3. 一个git服务器*(如github)*
4. 服务器的git账号

### 安装
将本插件放入**MCDR**目录下的`plugins`文件夹下即可

### 配置文件
```json
{
    "debug": false, // 是否开启调试信息
    "git_path": "git", // git安装路径
    "git_config": {
        "remote": "https://github.com/user/repo.git", // 远程地址
        "remote_name": "backup", // 远程分支名称
        "branch_name": "backup", // 本地分支名称
        "is_setup": false // 
    },
    "backup_interval": 86400, // 每次自动备份的时间间隔(sec), 设为0以禁用
    "last_backup_time": 0, // 上次进行备份的时间
    "push_interval": 86400, // 每次惰性推送的时间间隔(sec)(会在备份完成后检查是否推送), 设为0以禁用
    "last_push_time": 0, // 上次进行推送的时间
    "back_wait_time": 30, // 回档前的等待时间(秒)
    "backup_path": "./git_backup", // backup文件夹, git工作目录
    "cache_path": "./git_backup_cache", // 缓存文件夹, 会把回档之前的server文件夹
    "server_path": "./server", // 服务器文件夹
    "need_backup": [ // 需要备份的文件名
        "world"
    ],
    "ignores": [ // 忽略以下文件名
        "session.lock"
    ],
    "minimum_permission_level": { // 指令权限等级
        "help": 0,
        "status": 1,
        "list": 1,
        "make": 1,
        "back": 2,
        "confirm": 1,
        "abort": 1,
        "reload": 2,
        "save": 2,
        "push": 2,
        "pull": 2
    }
}
```

## 使用
### 指令

- `!!gbk help` 显示帮助信息
- `!!gbk status` 显示备份状态
- `!!gbk list [<limit>]` 列出所有/<limit>条备份
- `!!gbk make [<comment>]` 创建新备份
- `!!gbk back [:<index>|<hash id>]` 恢复到指定id
- `!!gbk confirm <id>` 确认回档
- `!!gbk abort` 取消回档
- `!!gbk reload` 重新加载配置文件
- `!!gbk save` 保存配置文件
- `!!gbk push` 将备份信息推送到远程服务器
- `!!gbk pull` 拉取远程服务器的备份信息

# English