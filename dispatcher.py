#!/usr/bin/env python3
"""
24/7 dispatcher living in the keep-alive repo.

Replaces the per-repo cron schedules of the non-24/7 workflows:
at the scheduled Tehran times it dispatches those workflows via the
GitHub API, using the DISPATCH_PAT secret (fine-grained PAT, Actions
read/write on the covered repos).

Behaviour (per the owner's spec):
- Runs continuously in a GitHub Actions job (max ~6h per job).
- Before the job cap (~5h40m in) it dispatches its own next run and
  exits cleanly (self-dispatch chain, like the 24/7 bots).
- Every day at 08:00 Tehran it restarts itself the same way, so it is
  freshly up before the 08:30 / 08:40 / 08:45 dispatches.
- At startup (e.g. right after a restart) it checks whether today's
  slots that have already passed actually have a run; if a slot was
  missed while it was down, it dispatches it immediately (catch-up).
- A slot counts as covered if a run of that workflow was created in
  Tehran time at or after slot-15min today (manual runs count).
- On SIGINT/SIGTERM it tries to hand over to a next run before exit.

Stdlib only. State is derived from the GitHub API, so restarts are safe.
"""

import json
import os
import signal
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

API = "https://api.github.com"
OWNER = "azcheryani-bot"
TOKEN = os.environ.get("DISPATCH_PAT", "")
TEHRAN = timezone(timedelta(hours=3, minutes=30))

# repo, workflow file, (hour, minute) Tehran, weekdays Tehran (Mon=0..Sun=6) or None = daily
TARGETS = [
    ("for-me", "relay.yml", (8, 30), None),
    ("portfolio-guardian", "run-guardian.yml", (8, 40), {5, 6, 0, 1, 2}),  # Sat-Wed
    ("scalper-test", "scalper_daily.yml", (8, 45), {5, 6, 0, 1, 2}),       # Sat-Wed
]
SELF_REPO = "keep-alive"
SELF_WORKFLOW = "dispatcher.yml"

START_MONO = time.monotonic()
MAX_RUNTIME = 5 * 3600 + 40 * 60  # self-dispatch at ~5h40m, before the 6h cap
RESTART_HOUR = 8                  # daily self-restart at 08:00 Tehran
LOOP_SLEEP = 30
COVER_CACHE_TTL = 240             # seconds to cache "slot covered" API checks

_stop = False


def log(msg):
    now = datetime.now(TEHRAN).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[dispatcher {now} Tehran] {msg}", flush=True)


def _handle_signal(signum, frame):
    global _stop
    _stop = True
    log(f"signal {signum} received; shutting down after handover attempt")


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def api(method, path, body=None):
    url = API + path
    data = None
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "keep-alive-dispatcher",
    }
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        log(f"API {method} {path} -> HTTP {e.code}: {raw[:300]}")
        return e.code, {}
    except Exception as e:  # network hiccup etc.
        log(f"API {method} {path} -> error: {e}")
        return 0, {}


def slot_dt(now_tehran, hm):
    return now_tehran.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)


def scheduled_today(now_tehran, weekdays):
    return weekdays is None or now_tehran.weekday() in weekdays


def slot_covered(repo, workflow, slot):
    """True if a run of workflow was created at/after slot-15min (Tehran)."""
    status, data = api(
        "GET",
        f"/repos/{OWNER}/{repo}/actions/workflows/{workflow}/runs?per_page=10",
    )
    if status != 200:
        return False
    threshold = slot - timedelta(minutes=15)
    for run in data.get("workflow_runs", []):
        created = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
        if created.astimezone(TEHRAN) >= threshold:
            return True
    return False


def dispatch(repo, workflow):
    status, _ = api(
        "POST",
        f"/repos/{OWNER}/{repo}/actions/workflows/{workflow}/dispatches",
        {"ref": "main"},
    )
    if status == 204:
        log(f"dispatched {repo}/{workflow}")
        return True
    log(f"dispatch FAILED for {repo}/{workflow} (HTTP {status})")
    return False


def self_dispatch():
    """Dispatch the next dispatcher run unless a successor is already queued.

    Our own run is the in_progress one, so only a *queued* run proves a
    successor exists (e.g. a bootstrap-cron run waiting behind us)."""
    status, data = api(
        "GET",
        f"/repos/{OWNER}/{SELF_REPO}/actions/workflows/{SELF_WORKFLOW}/runs?per_page=10",
    )
    if status == 200:
        for run in data.get("workflow_runs", []):
            if run.get("status") == "queued":
                log("successor already queued; skipping self-dispatch")
                return True
    for attempt in (1, 2):
        status, _ = api(
            "POST",
            f"/repos/{OWNER}/{SELF_REPO}/actions/workflows/{SELF_WORKFLOW}/dispatches",
            {"ref": "main"},
        )
        if status == 204:
            log("self-dispatch sent (next dispatcher run)")
            return True
        log(f"self-dispatch attempt {attempt} FAILED (HTTP {status})")
        if attempt == 1:
            time.sleep(30)
    return False


def next_restart_mono(start_tehran):
    """Monotonic deadline for the next 08:00-Tehran restart after start."""
    now = datetime.now(TEHRAN)
    today_restart = now.replace(hour=RESTART_HOUR, minute=0, second=0, microsecond=0)
    if now >= today_restart:
        target = today_restart + timedelta(days=1)
    else:
        target = today_restart
    return START_MONO + (target - start_tehran).total_seconds()


def main():
    if not TOKEN:
        log("DISPATCH_PAT is not set; cannot run")
        return 1
    start_tehran = datetime.now(TEHRAN)
    restart_deadline = next_restart_mono(start_tehran)
    log(f"dispatcher started; next 08:00-restart in "
        f"{int((restart_deadline - START_MONO) // 60)} min; "
        f"self-dispatch at {MAX_RUNTIME // 60} min runtime")

    covered_cache = {}   # (repo, date) -> (checked_mono, covered_bool)
    dispatched = set()   # (repo, date) dispatched by this run

    def is_covered(repo, workflow, slot, now_tehran):
        key = (repo, now_tehran.date())
        hit = covered_cache.get(key)
        if hit and time.monotonic() - hit[0] < COVER_CACHE_TTL:
            return hit[1]
        covered = slot_covered(repo, workflow, slot)
        covered_cache[key] = (time.monotonic(), covered)
        return covered

    # Startup catch-up: fire any of today's slots that passed with no run.
    now = datetime.now(TEHRAN)
    for repo, workflow, hm, weekdays in TARGETS:
        if not scheduled_today(now, weekdays):
            continue
        slot = slot_dt(now, hm)
        if now >= slot and not is_covered(repo, workflow, slot, now):
            log(f"catch-up: {repo} slot {hm[0]:02d}:{hm[1]:02d} passed with no run; dispatching")
            if dispatch(repo, workflow):
                dispatched.add((repo, now.date()))
                covered_cache[(repo, now.date())] = (time.monotonic(), True)
        elif now >= slot:
            log(f"catch-up check: {repo} slot {hm[0]:02d}:{hm[1]:02d} already covered")

    while not _stop:
        now_mono = time.monotonic()
        if now_mono - START_MONO >= MAX_RUNTIME:
            log("runtime cap reached; handing over")
            self_dispatch()
            return 0
        if now_mono >= restart_deadline:
            log("08:00 Tehran restart point; handing over to a fresh run")
            self_dispatch()
            return 0

        now = datetime.now(TEHRAN)
        for repo, workflow, hm, weekdays in TARGETS:
            if not scheduled_today(now, weekdays):
                continue
            if (repo, now.date()) in dispatched:
                continue
            slot = slot_dt(now, hm)
            if now >= slot and not is_covered(repo, workflow, slot, now):
                log(f"slot reached for {repo} ({hm[0]:02d}:{hm[1]:02d}); dispatching")
                if dispatch(repo, workflow):
                    dispatched.add((repo, now.date()))
                    covered_cache[(repo, now.date())] = (time.monotonic(), True)
        # sleep in small slices so signals are handled promptly
        for _ in range(LOOP_SLEEP):
            if _stop:
                break
            time.sleep(1)

    log("shutdown signal path; attempting handover")
    self_dispatch()
    return 0


if __name__ == "__main__":
    sys.exit(main())
