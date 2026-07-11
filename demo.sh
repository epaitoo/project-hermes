#!/usr/bin/env bash
#
# demo.sh — drives the Hermes crash-recovery story on a fixed clock so the
# Grafana GIF is repeatable. Recording stays manual: keep
# http://localhost:3000/d/hermes open and run your screen recorder; this script
# only drives the backend.
#
# The narrative: climb -> crash -> flatline -> WAL replay. On recovery the
# gauges (pending, dlq_size) snap back from the WAL while the counters
# (completed_total, attempt_failure_total, dead_letter_total) legitimately reset
# to zero. Verified: in-flight leased jobs rejoin pending — no work is lost.

set -euo pipefail

# ---- tunables: re-time the GIF here -----------------------------------------
RATE=15            # loadgen submit rate (jobs/s). Must outpace worker drain so
                   # pending accumulates and there is a crash to show.
FAILRATE=0.2       # fraction tagged poison -> exercises retry -> DLQ.
CLIMB_SECS=12      # how long gauges climb before the crash.
FLATLINE_SECS=15   # how long the broker stays down. >= 3x the 5s Prometheus
                   # scrape interval so the outage is 3-4 flat samples, not one.
SETTLE_SECS=10     # how long to run after recovery so the snap-back is visible.
PREROLL_SECS=5     # non-interactive fallback pause to start your recorder.
BROKER_URL="http://localhost:8080"
PROM_URL="http://localhost:9090"
GRAFANA_URL="http://localhost:3000"
# -----------------------------------------------------------------------------

cd "$(dirname "$0")"

TMPDIR="$(mktemp -d)"
LOADGEN_BIN="$TMPDIR/loadgen"
LOADGEN_LOG="$TMPDIR/loadgen.log"
LG_PID=""

cleanup() {
  if [[ -n "$LG_PID" ]]; then
    kill "$LG_PID" 2>/dev/null || true
  fi
  rm -rf "$TMPDIR"
}
trap cleanup EXIT INT TERM

log()  { printf '\n>>> [%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
snap() {
  curl -s --max-time 2 "$BROKER_URL/metrics" | awk '
    /^hermes_jobs_pending /{p=$2}/^hermes_jobs_leased /{l=$2}/^hermes_jobs_dlq_size /{d=$2}
    /^hermes_jobs_completed_total /{c=$2}/^hermes_jobs_attempt_failure_total /{f=$2}/^hermes_jobs_dead_letter_total /{dl=$2}
    END{printf "  pending=%s leased=%s dlq_size=%s | completed=%s attempt_failure=%s dead_letter=%s\n", p,l,d,c,f,dl}'
}

# Poll an endpoint until it answers (200 for a bare URL, or a specific code).
wait_ready() {
  local name="$1" url="$2" want="${3:-any}" i code
  for i in $(seq 1 40); do
    if [[ "$want" == "any" ]]; then
      curl -s --max-time 1 "$url" >/dev/null 2>&1 && return 0
    else
      code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "$url" 2>/dev/null || true)"
      [[ "$code" == "$want" ]] && return 0
    fi
    sleep 1
  done
  echo "$name did not become ready in time ($url)" >&2
  return 1
}

# Wait for the whole observability stack, not just the broker — otherwise the
# "open the dashboard" cue fires while Grafana is still booting and you race
# your own recorder. The Grafana check hits the provisioned dashboard itself
# (200 only once it is servable), which is stronger than /api/health.
wait_stack() {
  wait_ready broker     "$BROKER_URL/metrics"
  wait_ready prometheus "$PROM_URL/-/ready"
  wait_ready grafana    "$GRAFANA_URL/api/dashboards/uid/hermes" 200
}

# Give the operator time to position the browser and start recording. Waits for
# Enter interactively; falls back to a countdown when there is no TTY.
preroll() {
  if [[ -t 0 ]]; then
    read -r -p ">>> Set up your recorder + dashboard, then press Enter to start the climb... "
  else
    log "PREROLL: ${PREROLL_SECS}s to start your recorder (no TTY; using countdown)"
    sleep "$PREROLL_SECS"
  fi
}

# Build loadgen up front so compile time is not part of the climb window.
log "Building loadgen"
go build -o "$LOADGEN_BIN" ./cmd/loadgen

# Full reset every run: a clean, zeroed baseline is what makes the snap-back
# legible and take-to-take comparable. No opt-out by design.
log "Clean reset: wiping WAL + Prometheus/Grafana volumes, rebuilding broker"
docker compose down -v
docker compose up -d --build
log "Waiting for broker + Prometheus + Grafana dashboard to be ready"
wait_stack
log "Baseline (expect all zero):"; snap
echo "    Dashboard ready: $GRAFANA_URL/d/hermes"

preroll

log "CLIMB: driving rate=$RATE failrate=$FAILRATE for ${CLIMB_SECS}s (loadgen -> $LOADGEN_LOG)"
"$LOADGEN_BIN" -rate "$RATE" -failrate "$FAILRATE" -broker "$BROKER_URL" >"$LOADGEN_LOG" 2>&1 &
LG_PID=$!
sleep "$CLIMB_SECS"
log "Pre-crash state:"; snap

log "CRASH: stopping broker (restart policy suppressed) — holding ${FLATLINE_SECS}s flatline"
docker compose stop broker
sleep "$FLATLINE_SECS"

log "RECOVER: starting broker — WAL replays; gauges snap back, counters reset to 0"
docker compose start broker
wait_ready broker "$BROKER_URL/metrics"
log "Post-recovery state (counters back at 0, gauges restored from WAL):"; snap

log "SETTLE: loadgen resumes for ${SETTLE_SECS}s so recovery is visible on the graph"
sleep "$SETTLE_SECS"

log "Done. Stopping loadgen; stack stays up. Re-run this script for another take."
