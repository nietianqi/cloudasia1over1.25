# cloudasia1over1.25

赛前 5 分钟扫描 Cloudbet 足球赛事，筛选“强弱分明”的深盘比赛池，供后续滚球策略（如 OU 1.25 二次触发）使用。

## 当前实现（预扫描层 v1）

- 每轮扫描只看未来 `0~5` 分钟开赛的足球赛事。
- 读取 `Asian Handicap` 主盘口（从同一盘口线的 home/away 报价中识别）。
- 纳入条件：
  - 任一方让球绝对值 `>= 1.0`
  - 强队水位 `>= 1.6`
- 分层：
  - `A`: 1.0
  - `B`: 1.25
  - `C`: 1.5
  - `D`: 2.0
  - `E`: >= 2.25
- 标准化输出字段：
  - `favorite_team`, `underdog_team`, `favorite_side`, `favorite_line_abs`
  - `fav_odds`, `dog_odds`, `pre_match_bucket`
  - `watchlist_flag=true`, `strategy_tag=PRE_FAVORITE_DEEP_AH`

## 安装

```bash
python -m pip install -e .
```

## 单次扫描

```bash
cloudasia-scan --once --api-key <YOUR_CLOUDBET_API_KEY>
```

可选参数：

```bash
cloudasia-scan --once \
  --minutes-to-kickoff-max 5 \
  --min-favorite-line-abs 1.0 \
  --min-favorite-odds 1.6 \
  --api-key <YOUR_CLOUDBET_API_KEY>
```

## 持续扫描（每分钟）

```bash
cloudasia-scan --interval-seconds 60
```

或使用环境变量：

```bash
set CLOUDBET_API_KEY=<YOUR_CLOUDBET_API_KEY>
cloudasia-scan --once
```

## 输出到 JSONL

```bash
cloudasia-scan --once --output data/watchlist.jsonl
```

## 说明

- `ah_main_line` 使用主队视角（home handicap）。例如：
  - `-1.25` 表示主队让 1.25
  - `+1.25` 表示客队让 1.25
- Cloudbet 接口字段在不同版本里有轻微差异，代码已兼容 `soccer.asian_handicap` 与 `soccer.asianHandicap`。
