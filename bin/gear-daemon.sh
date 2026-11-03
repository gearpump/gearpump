#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

umask 077
USAGE="Usage: gear-daemon.sh (start|stop|stop-all) (local|master|worker|services) [args]"
OPERATION=${1:-}
DAEMON=${2:-}
ARGS=("${@:3}")
fail() { echo "Error: $*" >&2; exit 1; }
case "$DAEMON" in
  local) main_class=io.gearpump.cluster.main.Local ;;
  master) main_class=io.gearpump.cluster.main.Master ;;
  worker) main_class=io.gearpump.cluster.main.Worker ;;
  services) main_class=io.gearpump.services.main.Services ;;
  *) fail "$USAGE" ;;
esac
case "$OPERATION" in start|stop|stop-all) ;; *) fail "$USAGE" ;; esac
bin=$(cd "$(dirname "$0")" && pwd -P) || exit 1
# shellcheck source=/dev/null
. "$bin/config.sh"
current_uid=$(id -u)
GEARPUMP_IDENT_STRING=${GEARPUMP_IDENT_STRING:-$(id -un)}
[[ "$GEARPUMP_IDENT_STRING" =~ ^[A-Za-z0-9._-]+$ ]] || fail "Invalid daemon identifier"

owner_mode() {
  if [ "$(uname -s)" = Darwin ]; then stat -f '%u %Lp' "$1"
  else stat -c '%u %a' "$1"; fi
}
check_private() {
  local info owner mode
  [ ! -L "$1" ] || fail "Refusing symlink: $1"
  info=$(owner_mode "$1") || fail "Cannot inspect $1"
  read -r owner mode <<< "$info"
  [ "$owner" = "$current_uid" ] || fail "Foreign-owned daemon state: $1"
  (( (8#$mode & 077) == 0 )) || fail "Daemon state must be private (0700/0600): $1"
}
prepare_dir() {
  [ ! -L "$1" ] || fail "Refusing symlink directory: $1"
  # umask 077 also makes any newly created parent directories private.
  mkdir -p "$1" || fail "Cannot create $1"
  [ -d "$1" ] || fail "Not a directory: $1"
  check_private "$1"
}
prepare_dir "$GEARPUMP_PID_DIR"
prepare_dir "$GEARPUMP_LOG_DIR"
pid="$GEARPUMP_PID_DIR/gear-$GEARPUMP_IDENT_STRING-$DAEMON.pid"
if [ -e "$pid" ] || [ -L "$pid" ]; then
  [ -f "$pid" ] || fail "Not a regular PID file: $pid"
  check_private "$pid"
fi
lock="$pid.lock"
mkdir -m 700 "$lock" 2>/dev/null || fail "Daemon operation locked: $lock"
trap 'rmdir "$lock" 2>/dev/null || true' EXIT

process_stamp() { ps -p "$1" -o lstart= | sed 's/^[[:space:]]*//'; }
process_owner() { ps -p "$1" -o uid= | tr -d '[:space:]'; }
process_role() {
  local command
  command=$(ps -p "$1" -o args=) || return 1
  [[ "$command" == *"$main_class"* ]]
}
validate_record() {
  local p owner stamp role extra
  IFS='|' read -r p owner stamp role extra <<< "$1"
  [[ "$p" =~ ^[1-9][0-9]*$ ]] && [ "$owner" = "$current_uid" ] &&
    [ "$role" = "$DAEMON" ] && [ -n "$stamp" ] && [ -z "$extra" ] ||
    fail "Invalid or legacy PID record; remove only after verifying the old daemon"
  if kill -0 "$p" 2>/dev/null; then
    if [ "$(process_owner "$p")" != "$owner" ] ||
        [ "$(process_stamp "$p")" != "$stamp" ] || ! process_role "$p"; then
      fail "PID identity mismatch; refusing to signal $p"
    fi
  fi
}
stop_record() {
  local record=$1 p
  validate_record "$record"
  p=${record%%|*}
  if kill -0 "$p" 2>/dev/null; then
    echo "Stopping $DAEMON daemon (pid: $p)."
    kill "$p" || fail "Cannot stop $p"
  fi
}

case "$OPERATION" in
  start)
    if [ -f "$pid" ]; then
      while IFS= read -r record; do validate_record "$record"; done < "$pid"
    fi
    out=$(mktemp "$GEARPUMP_LOG_DIR/gearpump-$GEARPUMP_IDENT_STRING-$DAEMON.XXXXXXXX") || exit 1
    echo "Starting $DAEMON daemon; log: $out"
    "$bin/$DAEMON" "${ARGS[@]}" > "$out" 2>&1 < /dev/null &
    mypid=$!
    ready=false
    for _ in {1..50}; do
      if process_role "$mypid"; then ready=true; break; fi
      kill -0 "$mypid" 2>/dev/null || break
      sleep 0.1
    done
    [ "$ready" = true ] || fail "Daemon did not start; inspect $out"
    stamp=$(process_stamp "$mypid")
    owner=$(process_owner "$mypid")
    [ "$owner" = "$current_uid" ] && [ -n "$stamp" ] || fail "Cannot identify daemon"
    if [ ! -e "$pid" ]; then (set -C; : > "$pid") || exit 1; fi
    printf '%s|%s|%s|%s\n' "$mypid" "$owner" "$stamp" "$DAEMON" >> "$pid"
    ;;
  stop)
    if [ -f "$pid" ]; then
      record=$(tail -n 1 "$pid")
      [ -n "$record" ] && stop_record "$record"
      replacement=$(mktemp "$GEARPUMP_PID_DIR/pid-update.XXXXXXXX") || exit 1
      sed '$d' "$pid" > "$replacement"
      if [ -s "$replacement" ]; then mv "$replacement" "$pid"
      else rm "$replacement" "$pid"; fi
    fi
    ;;
  stop-all)
    if [ -f "$pid" ]; then
      while IFS= read -r record; do validate_record "$record"; done < "$pid"
      while IFS= read -r record; do stop_record "$record"; done < "$pid"
      rm "$pid"
    fi
    ;;
esac
