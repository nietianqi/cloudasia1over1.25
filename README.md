# cloudasia1over1.25

Cloudbet football strategy runner with:

- Layer 1: pre-match deep AH scan
- Layer 2: live TG 1.25 trigger monitor
- Optional: continuous pipeline with real betting + bankroll controls

## Quick Start

1. Edit [config.toml](F:/cloudasia1over1.25/config.toml)
2. Set environment variable:

```bash
set CLOUDBET_API_KEY=your_api_key
```

3. Run:

```bash
python run.py
```

## Modes

- `prematch`: only pre-match scan
- `live`: only live monitor (requires existing watchlist file)
- `pipeline`: one prematch run + one live run
- `pipeline_continuous`: full continuous loop (recommended for production)

Set in:

```toml
[app]
mode = "pipeline_continuous"
```

## Real-Money Betting Safety Gate

Real betting is controlled by `[betting]`:

```toml
[betting]
enabled = true
dry_run = false
require_live_ack = true
live_ack_phrase = "LIVE_BETTING_ACK"
live_ack_token = ""
```

When `dry_run=false`, bets are still blocked until:

`live_ack_token == live_ack_phrase`

This prevents accidental live betting.

## Bankroll Management (Configurable)

Configured in `[money]`:

- Fractional Kelly sizing
- Daily loss stop
- Max drawdown stop
- Max concurrent exposure
- Max consecutive losses
- Max daily bet count
- Cooldown between bets
- Reserve bankroll percent

Stake can be `0` when edge is insufficient (no forced low-quality bets).

## Output Files

Default output directory: `data/`

- `watchlist.jsonl`
- `live_signals.jsonl`
- `bet_log.jsonl`
- `bankroll.json`

## Tests

```bash
python -m pytest -q
```
