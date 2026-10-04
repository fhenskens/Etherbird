#!/usr/bin/env bash
# Supply release examples directories built before and after the change.
set -euo pipefail
baseline="$(realpath "$1")"
candidate="$(realpath "$2")"
output="${3:-target/mqtt-dispatch}"
mkdir -p "$output"
for runtime in current two default; do
  for pass in 0 1; do
    if [ "$pass" = 0 ]; then phases=(baseline candidate); else phases=(candidate baseline); fi
    for phase in "${phases[@]}"; do
      if [ "$phase" = baseline ]; then
        build="$baseline"
        programs=(mqtt_without_etherbird mqtt_with_supervisor mqtt_with_etherbird)
      else
        build="$candidate"
        programs=(mqtt_with_etherbird mqtt_with_supervisor mqtt_without_etherbird)
      fi
      for program in "${programs[@]}"; do
        prefix="$output/linux-${runtime}-${phase}-${program}-${pass}"
        /usr/bin/time -v -o "${prefix}.time" "$build/$program" \
          --benchmark --runtime "$runtime" > "${prefix}.csv"
      done
    done
  done
done
