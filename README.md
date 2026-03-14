# cloudasia1over1.25

Cloudbet football strategy runner with:

- Layer 1: pre-match deep AH scan
- Layer 2: live TG 1.25 trigger monitor (single rule only)
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

## Live Rule (Current)

The live layer now keeps only one condition:

- Main Total Goals line equals `trigger_total_line` (default `1.25`)

When line hits `1.25`, signal is marked as `qualified` immediately.

## Real-Money Betting Safety Gate

Real betting is controlled by `[betting]`:

```toml
[betting]
enabled = true
dry_run = false
require_live_ack = true
live_ack_phrase = "LIVE_BETTING_ACK"
live_ack_token = "LIVE_BETTING_ACK"
```

When `dry_run=false`, real bets are allowed only if:

`live_ack_token == live_ack_phrase`

To lock live betting again, set `live_ack_token = ""`.

## Startup Preflight (Important)

In `pipeline_continuous`, startup now performs strict checks before entering the loop:

- API key must exist (`cloudbet.api_key` or env `CLOUDBET_API_KEY`)
- Odds API authentication must succeed
- Account API info + balance must be readable

If any of those fail, the process exits immediately (no endless unauthorized spam loop).

## Real Account Balance Sync

When running with `dry_run=false`, bankroll can sync from Cloudbet account balance at startup.

Config in `[money]`:

```toml
sync_with_account_balance = true
account_balance_currency = "USDT"
```

With sync enabled, local bankroll state is replaced by account balance on startup.

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

`config.toml` defaults are now a conservative production profile:

- 15% fractional Kelly
- 20% reserve bankroll
- 2% max stake per bet, 25 USDT hard cap
- 4% daily loss stop, 12% max drawdown stop
- 8% max concurrent exposure
- max 3 consecutive losses, max 8 bets/day, 60s cooldown

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
