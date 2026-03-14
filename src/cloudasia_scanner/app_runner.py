from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import os
import time
from typing import Any

from .bet_client import BetClient, BetConfig
from .cloudbet_client import ACCOUNT_BASE_URL, SPORTS_ODDS_BASE_URL, CloudbetClient
from .config_utils import as_bool, as_float, as_int, load_toml_config, resolve_path
from .live_monitor import LiveLayerTwoMonitor, LiveMonitorConfig, load_watchlist
from .money_manager import MoneyConfig, MoneyManager
from .pipeline import PipelineConfig, PipelineRunner
from .prematch_scan import PreMatchScanner, ScanConfig


DEFAULT_ALLOWED_SCORES = {(0, 0), (1, 0), (0, 1)}


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _resolve_cloud_api_settings(config: dict[str, Any]) -> tuple[str | None, str, str, str, str]:
    cloud = config.get("cloudbet", {}) if isinstance(config.get("cloudbet"), dict) else {}
    api_key_env = str(cloud.get("api_key_env", "CLOUDBET_API_KEY"))
    raw_api_key = cloud.get("api_key")
    env_api_key = os.getenv(api_key_env)
    api_key = str(raw_api_key).strip() if isinstance(raw_api_key, str) and raw_api_key.strip() else None
    if not api_key:
        api_key = str(env_api_key).strip() if isinstance(env_api_key, str) and env_api_key.strip() else None
    api_key_header = str(cloud.get("api_key_header", "X-API-Key"))
    base_url = str(cloud.get("base_url", SPORTS_ODDS_BASE_URL))
    account_base_url = str(cloud.get("account_base_url", ACCOUNT_BASE_URL))
    return api_key, api_key_env, api_key_header, base_url, account_base_url


def _append_jsonl(output_path: Path, rows: list[dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def _build_client(config: dict[str, Any]) -> CloudbetClient:
    api_key, _, api_key_header, base_url, account_base_url = _resolve_cloud_api_settings(config)

    return CloudbetClient(
        base_url=str(base_url),
        api_key=str(api_key) if api_key else None,
        api_key_header=str(api_key_header),
        account_base_url=str(account_base_url),
    )


def _startup_preflight(
    config: dict[str, Any],
    client: CloudbetClient,
    bet_client: BetClient,
    money_manager: MoneyManager,
) -> None:
    api_key, api_key_env, _, _, _ = _resolve_cloud_api_settings(config)
    if not api_key:
        raise PermissionError(
            "Missing Cloudbet API key. "
            f"Set environment variable `{api_key_env}` or fill `cloudbet.api_key` in config.toml."
        )

    live_unlocked = (
        bet_client.config.enabled
        and not bet_client.config.dry_run
        and (
            (not bet_client.config.require_live_ack)
            or (bet_client.config.live_ack_token == bet_client.config.live_ack_phrase)
        )
    )
    print(
        f"[{_ts()}] [STARTUP] key_header={client.api_key_header} "
        f"dry_run={bet_client.config.dry_run} live_unlocked={live_unlocked}",
        flush=True,
    )

    # Fail fast before the endless pipeline loop if credentials are wrong.
    client.validate_odds_auth()
    print(f"[{_ts()}] [STARTUP] Cloudbet odds API auth OK.", flush=True)

    money_section = config.get("money", {}) if isinstance(config.get("money"), dict) else {}
    balance_currency = str(
        money_section.get("account_balance_currency", bet_client.config.currency)
    ).strip().upper() or bet_client.config.currency
    sync_from_account = as_bool(
        money_section.get("sync_with_account_balance"),
        not bet_client.config.dry_run,
    )

    account_label = "unknown"
    try:
        account_info = client.get_account_info()
        nickname = account_info.get("nickname") if isinstance(account_info, dict) else None
        account_uuid = account_info.get("uuid") if isinstance(account_info, dict) else None
        account_label = str(nickname or account_uuid or "unknown")
        balance = client.get_account_balance(balance_currency)
    except Exception as exc:
        # Only hard-fail when the user explicitly requested account sync.
        # In live mode without sync, log a warning and continue with local bankroll.
        if sync_from_account:
            raise RuntimeError(f"Cloudbet Account API startup check failed: {exc}") from exc
        print(
            f"[{_ts()}] [STARTUP] account API unavailable (using local bankroll): {exc}",
            flush=True,
        )
        return

    if balance is None:
        # Hard-fail only when sync was explicitly requested.
        if sync_from_account:
            raise RuntimeError(
                f"Cloudbet Account API returned no balance for currency={balance_currency}. "
                "Check account currency availability or set money.sync_with_account_balance=false."
            )
        print(
            f"[{_ts()}] [STARTUP] {balance_currency} balance unavailable; "
            f"using local bankroll. {money_manager.summary_line()}",
            flush=True,
        )
        return

    print(
        f"[{_ts()}] [STARTUP] account={account_label} "
        f"{balance_currency}_balance={balance:.8f}",
        flush=True,
    )

    if sync_from_account:
        money_manager.sync_bankroll_from_account(balance)
        print(
            f"[{_ts()}] [STARTUP] bankroll synced from account "
            f"({balance_currency} {balance:.2f}). {money_manager.summary_line()}",
            flush=True,
        )
    else:
        print(
            f"[{_ts()}] [STARTUP] bankroll uses local state only. "
            f"{money_manager.summary_line()}",
            flush=True,
        )


def _parse_allowed_scores(raw: Any) -> set[tuple[int, int]]:
    if not isinstance(raw, list):
        return set(DEFAULT_ALLOWED_SCORES)
    parsed: set[tuple[int, int]] = set()
    for item in raw:
        if isinstance(item, str):
            if ":" not in item:
                continue
            left, right = item.split(":", 1)
            try:
                parsed.add((int(left), int(right)))
            except ValueError:
                continue
        elif isinstance(item, list) and len(item) == 2:
            try:
                parsed.add((int(item[0]), int(item[1])))
            except (TypeError, ValueError):
                continue
    return parsed or set(DEFAULT_ALLOWED_SCORES)


def _run_prematch(config: dict[str, Any], base_dir: Path, client: CloudbetClient) -> None:
    section = config.get("prematch", {}) if isinstance(config.get("prematch"), dict) else {}
    once = as_bool(section.get("once"), True)
    interval_seconds = as_int(section.get("interval_seconds"), 60)
    output_path = resolve_path(base_dir, section.get("output"), default="data/watchlist.jsonl")

    scan_config = ScanConfig(
        minutes_to_kickoff_max=as_float(section.get("minutes_to_kickoff_max"), 5.0),
        min_favorite_line_abs=as_float(section.get("min_favorite_line_abs"), 1.0),
        min_favorite_odds=as_float(section.get("min_favorite_odds"), 1.6),
        verbose=as_bool(section.get("verbose"), False),
    )
    scanner = PreMatchScanner(client=client, config=scan_config)

    while True:
        records = [record.to_dict() for record in scanner.scan_once()]
        print(json.dumps(records, ensure_ascii=False, indent=2))
        if output_path is not None:
            _append_jsonl(output_path, records)

        if once:
            return
        now = datetime.now(timezone.utc).isoformat()
        print(f"[{now}] prematch records={len(records)}, next scan in {interval_seconds}s")
        time.sleep(interval_seconds)


def _run_live(config: dict[str, Any], base_dir: Path, client: CloudbetClient) -> None:
    section = config.get("live", {}) if isinstance(config.get("live"), dict) else {}
    watchlist_path = resolve_path(base_dir, section.get("watchlist"), default="data/watchlist.jsonl")
    if watchlist_path is None or not watchlist_path.exists():
        raise FileNotFoundError(f"Watchlist file not found: {watchlist_path}")

    watchlist = load_watchlist(watchlist_path)
    if not watchlist:
        raise ValueError(f"No valid watchlist rows in {watchlist_path}")

    output_path = resolve_path(base_dir, section.get("output"), default="data/live_signals.jsonl")
    allowed_scores = _parse_allowed_scores(section.get("allowed_scores"))

    live_config = LiveMonitorConfig(
        trigger_total_line=as_float(section.get("trigger_total_line"), 1.25),
        primary_minute_start=as_int(section.get("primary_minute_start"), 55),
        primary_minute_end=as_int(section.get("primary_minute_end"), 72),
        allowed_scores=allowed_scores,
        min_seconds_since_reopen=as_float(section.get("min_seconds_since_reopen"), 20.0),
        max_line_jumps_last_60s=as_int(section.get("max_line_jumps_last_60s"), 1),
        max_odds_jumps_last_60s=as_int(section.get("max_odds_jumps_last_60s"), 3),
        min_over_odds=as_float(section.get("min_over_odds"), 1.8),
        normal_poll_interval_seconds=as_int(section.get("normal_interval_seconds"), 15),
        fast_poll_interval_seconds=as_int(section.get("fast_interval_seconds"), 5),
        fast_poll_line_threshold=as_float(section.get("fast_line_threshold"), 1.75),
    )
    monitor = LiveLayerTwoMonitor(client=client, watchlist=watchlist, config=live_config)
    once = as_bool(section.get("once"), True)

    while True:
        signals = [signal.to_dict() for signal in monitor.monitor_once()]
        print(json.dumps(signals, ensure_ascii=False, indent=2))
        if output_path is not None:
            _append_jsonl(output_path, signals)

        if once:
            return
        poll_seconds = monitor.recommended_poll_interval_seconds()
        now = datetime.now(timezone.utc).isoformat()
        print(f"[{now}] live signals={len(signals)}, next poll in {poll_seconds}s")
        time.sleep(poll_seconds)


def _build_live_monitor(config: dict[str, Any], client: CloudbetClient) -> LiveLayerTwoMonitor:
    section = config.get("live", {}) if isinstance(config.get("live"), dict) else {}
    allowed_scores = _parse_allowed_scores(section.get("allowed_scores"))
    live_config = LiveMonitorConfig(
        trigger_total_line=as_float(section.get("trigger_total_line"), 1.25),
        primary_minute_start=as_int(section.get("primary_minute_start"), 55),
        primary_minute_end=as_int(section.get("primary_minute_end"), 72),
        allowed_scores=allowed_scores,
        min_seconds_since_reopen=as_float(section.get("min_seconds_since_reopen"), 20.0),
        max_line_jumps_last_60s=as_int(section.get("max_line_jumps_last_60s"), 1),
        max_odds_jumps_last_60s=as_int(section.get("max_odds_jumps_last_60s"), 3),
        min_over_odds=as_float(section.get("min_over_odds"), 1.8),
        normal_poll_interval_seconds=as_int(section.get("normal_interval_seconds"), 15),
        fast_poll_interval_seconds=as_int(section.get("fast_interval_seconds"), 5),
        fast_poll_line_threshold=as_float(section.get("fast_line_threshold"), 1.75),
    )
    return LiveLayerTwoMonitor(client=client, watchlist={}, config=live_config)


def _build_scanner(config: dict[str, Any], client: CloudbetClient) -> PreMatchScanner:
    section = config.get("prematch", {}) if isinstance(config.get("prematch"), dict) else {}
    scan_config = ScanConfig(
        minutes_to_kickoff_max=as_float(section.get("minutes_to_kickoff_max"), 5.0),
        min_favorite_line_abs=as_float(section.get("min_favorite_line_abs"), 1.0),
        min_favorite_odds=as_float(section.get("min_favorite_odds"), 1.6),
        verbose=as_bool(section.get("verbose"), False),
    )
    return PreMatchScanner(client=client, config=scan_config)


def _build_money_manager(config: dict[str, Any], base_dir: Path) -> MoneyManager:
    section = config.get("money", {}) if isinstance(config.get("money"), dict) else {}
    bankroll_file_raw = section.get("bankroll_file", "data/bankroll.json")
    bankroll_path = resolve_path(base_dir, bankroll_file_raw, default="data/bankroll.json")
    bankroll_file = str(bankroll_path) if bankroll_path else "data/bankroll.json"

    money_config = MoneyConfig(
        initial_bankroll=as_float(section.get("initial_bankroll"), 500.0),
        kelly_fraction=as_float(section.get("kelly_fraction"), 0.25),
        base_win_rate=as_float(section.get("base_win_rate"), 0.55),
        min_win_rate=as_float(section.get("min_win_rate"), 0.50),
        max_win_rate=as_float(section.get("max_win_rate"), 0.70),
        quality_adjustment=as_float(section.get("quality_adjustment"), 0.07),
        min_kelly_edge=as_float(section.get("min_kelly_edge"), 0.0),
        reserve_bankroll_pct=as_float(section.get("reserve_bankroll_pct"), 0.0),
        max_stake_pct=as_float(section.get("max_stake_pct"), 0.05),
        min_stake=as_float(section.get("min_stake"), 5.0),
        max_stake=as_float(section.get("max_stake"), 50.0),
        force_min_stake=as_bool(section.get("force_min_stake"), False),
        daily_loss_limit_pct=as_float(section.get("daily_loss_limit_pct"), 0.10),
        max_drawdown_pct=as_float(section.get("max_drawdown_pct"), 0.20),
        max_concurrent_exposure_pct=as_float(section.get("max_concurrent_exposure_pct"), 0.25),
        max_consecutive_losses=as_int(section.get("max_consecutive_losses"), 5),
        max_daily_bets=as_int(section.get("max_daily_bets"), 20),
        bet_cooldown_seconds=as_int(section.get("bet_cooldown_seconds"), 15),
        bankroll_file=bankroll_file,
        auto_settle_after_minutes=as_int(section.get("auto_settle_after_minutes"), 130),
    )
    return MoneyManager(config=money_config)


def _build_bet_client(config: dict[str, Any], api_key: str | None) -> BetClient:
    section = config.get("betting", {}) if isinstance(config.get("betting"), dict) else {}
    bet_config = BetConfig(
        enabled=as_bool(section.get("enabled"), True),
        dry_run=as_bool(section.get("dry_run"), True),
        require_live_ack=as_bool(section.get("require_live_ack"), True),
        live_ack_phrase=str(section.get("live_ack_phrase", "LIVE_BETTING_ACK")),
        live_ack_token=str(section.get("live_ack_token", "")),
        stake_per_bet=as_float(section.get("stake_per_bet"), 10.0),
        currency=str(section.get("currency", "USDT")),
        min_accepted_price=as_float(section.get("min_accepted_price"), 1.78),
        max_active_bets=as_int(section.get("max_active_bets"), 5),
    )
    return BetClient(api_key=api_key, config=bet_config)


def _run_pipeline_continuous(config: dict[str, Any], base_dir: Path, client: CloudbetClient) -> None:
    api_key, _, _, _, _ = _resolve_cloud_api_settings(config)

    pipeline_section = config.get("pipeline", {}) if isinstance(config.get("pipeline"), dict) else {}
    output_dir_raw = pipeline_section.get("output_dir", "data")
    output_dir = resolve_path(base_dir, output_dir_raw, default="data") or (base_dir / "data")

    pipeline_config = PipelineConfig(
        prematch_interval_seconds=as_int(pipeline_section.get("prematch_interval_seconds"), 60),
        output_dir=output_dir,
        persist_watchlist=as_bool(pipeline_section.get("persist_watchlist"), True),
        persist_signals=as_bool(pipeline_section.get("persist_signals"), True),
        persist_bets=as_bool(pipeline_section.get("persist_bets"), True),
        finished_cleanup_interval_seconds=as_int(
            pipeline_section.get("finished_cleanup_interval_seconds"),
            300,
        ),
        settlement_check_interval_seconds=as_int(
            pipeline_section.get("settlement_check_interval_seconds"),
            300,
        ),
    )

    scanner = _build_scanner(config, client)
    monitor = _build_live_monitor(config, client)
    bet_client = _build_bet_client(config, str(api_key) if api_key else None)
    money_manager = _build_money_manager(config, base_dir)
    _startup_preflight(config, client, bet_client, money_manager)

    runner = PipelineRunner(
        scanner=scanner,
        monitor=monitor,
        bet_client=bet_client,
        money_manager=money_manager,
        config=pipeline_config,
    )
    runner.run_forever()


def run_from_config(config_path: Path | None = None) -> None:
    config, path = load_toml_config(config_path)
    base_dir = path.parent
    app_section = config.get("app", {}) if isinstance(config.get("app"), dict) else {}
    mode = str(app_section.get("mode", "prematch")).strip().lower()

    client = _build_client(config)

    if mode == "prematch":
        _run_prematch(config, base_dir, client)
        return
    if mode == "live":
        _run_live(config, base_dir, client)
        return
    if mode == "pipeline":
        prematch_section = config.get("prematch", {}) if isinstance(config.get("prematch"), dict) else {}
        if not as_bool(prematch_section.get("once"), True):
            raise ValueError("For app.mode=pipeline, set prematch.once=true.")
        _run_prematch(config, base_dir, client)
        _run_live(config, base_dir, client)
        return
    if mode == "pipeline_continuous":
        _run_pipeline_continuous(config, base_dir, client)
        return

    raise ValueError(
        "Invalid app.mode in config.toml. "
        "Use: prematch / live / pipeline / pipeline_continuous"
    )


def main() -> None:
    try:
        run_from_config()
    except PermissionError as exc:
        print(str(exc))
        print("Hint: set CLOUDBET_API_KEY or fill cloudbet.api_key in config.toml.")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"Run failed: {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
