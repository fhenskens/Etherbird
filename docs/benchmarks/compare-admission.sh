#!/usr/bin/env bash
# Compare separately built release directories containing deps/pool_dispatch-*.
set -euo pipefail
baseline="$(realpath "$1")"
candidate="$(realpath "$2")"
output="${3:-target/admission-results}"
mkdir -p "$output"
benchmark() {
  local newest="" possible
  for possible in "$1"/deps/pool_dispatch-*; do
    if [ -f "$possible" ] && [ -x "$possible" ] && { [ -z "$newest" ] || [ "$possible" -nt "$newest" ]; }; then newest="$possible"; fi
  done
  [ -n "$newest" ] || { echo "Missing benchmark in $1" >&2; return 1; }
  printf '%s\n' "$newest"
}
for runtime in current two default; do
  for pass in 0 1; do
    if [ "$pass" = 0 ]; then phases=(baseline candidate); else phases=(candidate baseline); fi
    for phase in "${phases[@]}"; do
      if [ "$phase" = baseline ]; then build="$baseline"; else build="$candidate"; fi
      binary="$(benchmark "$build")"
      prefix="$output/linux-${runtime}-${phase}-${pass}"
      /usr/bin/time -v -o "${prefix}.time" "$binary" --run --runtime "$runtime" > "${prefix}.csv"
    done
    echo "Completed allocation comparison: $runtime pass $pass"
  done
done
