# cloudasia1over1.25

基于 Cloudbet 的两层策略工具：

- 第一层：赛前 5 分钟深盘扫描（`-1.0` 及更深，且强队水位最低 `1.6`）
- 第二层：滚球主大小球 `1.25` 触发监控（状态机 + 过滤）

## 一键运行（配置文件方式）

你只需要改根目录的 [config.toml](F:/cloudasia1over1.25/config.toml)，然后运行：

```bash
python run.py
```

`run.py` 会自动读取 `config.toml`，不需要再传命令参数。

## PyCharm 推荐设置

1. 打开项目根目录：`F:\cloudasia1over1.25`
2. 右键 `src` -> `Mark Directory as` -> `Sources Root`
3. 新建 Run Configuration：
   - Script path: [run.py](F:/cloudasia1over1.25/run.py)
   - Working directory: `F:\cloudasia1over1.25`
4. 点击运行

## 配置文件说明

配置文件在 [config.toml](F:/cloudasia1over1.25/config.toml)。

- `app.mode`:
  - `prematch`: 只跑第一层
  - `live`: 只跑第二层
  - `pipeline`: 先跑第一层，再跑第二层（要求 `prematch.once=true`）
- `cloudbet.api_key`: 可直接写 key，也可留空并使用环境变量 `CLOUDBET_API_KEY`
- `prematch.*`: 第一层参数
- `live.*`: 第二层参数

### 常用切换

- 第一层循环扫描：`prematch.once = false`
- 第二层持续监控：`live.once = false`
- 第二层 watchlist 输入文件：`live.watchlist = "data/watchlist.jsonl"`

## 命令行入口（保留）

```bash
cloudasia-scan --help
cloudasia-live-monitor --help
cloudasia-run
```

## 当前实现要点

- 深盘分层：A/B/C/D/E = `1.0/1.25/1.5/2.0/2.25+`
- 第二层信号：
  - `TG125_LATE_FAVORITE_SIGNAL`（过滤通过）
  - `TG125_LATE_FAVORITE_WATCH`（触发但冷却中或被过滤）
- 第二层状态机：
  - `WATCHING -> TRIGGERED -> COOLING/QUALIFIED/REJECTED`

## 测试

```bash
python -m pytest -q
```
