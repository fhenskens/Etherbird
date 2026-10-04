#!/usr/bin/env bash
# Run as the ordinary build user; supply PERF if perf is not on PATH.
# sudo is needed for kernel stacks on hosts with restrictive perf permissions.
set -euo pipefail
cd "$(dirname "$0")/../.."
profile_dir="${PROFILE_DIR:-target/mqtt-profile}"
perf_tool="${PERF:-perf}"
mkdir -p "$profile_dir"
CARGO_TARGET_DIR="$profile_dir/build" CARGO_PROFILE_RELEASE_DEBUG=1 \
  RUSTFLAGS="${RUSTFLAGS:-} -Cforce-frame-pointers=yes" cargo build --features pool --release --locked \
  --example mqtt_without_etherbird --example mqtt_with_supervisor --example mqtt_with_etherbird
for runtime in default current; do
  for program in mqtt_without_etherbird mqtt_with_supervisor mqtt_with_etherbird; do
    prefix="$profile_dir/${runtime}-${program}"
    sudo --preserve-env=LD_LIBRARY_PATH "$perf_tool" stat -o "${prefix}.stat" \
      -e task-clock,context-switches,cpu-migrations,cycles,instructions -- \
      "$profile_dir/build/release/examples/$program" --benchmark --runtime "$runtime" > "${prefix}.csv"
    sudo --preserve-env=LD_LIBRARY_PATH "$perf_tool" record -q -e cpu-clock -F 499 \
      --call-graph fp -o "${prefix}.data" -- \
      "$profile_dir/build/release/examples/$program" --benchmark --runtime "$runtime" > "${prefix}-sampled.csv"
    # -f accommodates ownership reported by Windows-mounted WSL filesystems.
    sudo --preserve-env=LD_LIBRARY_PATH "$perf_tool" report -f --stdio --no-children \
      --call-graph none --percent-limit 0 -i "${prefix}.data" > "${prefix}-flat.report"
    sudo --preserve-env=LD_LIBRARY_PATH "$perf_tool" report -f --stdio --children \
      --call-graph none --percent-limit 1 -i "${prefix}.data" > "${prefix}-inclusive.report"
  done
done
