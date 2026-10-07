# Shared by the examples:* tasks. Each example prints a final ok: line once its checks pass.
run_example() {
  local name=$1
  shift
  local out
  if ! out=$("$@"); then
    printf '%s\n' "$out"
    exit 1
  fi
  printf '%s\n' "$out"
  grep -q '^ok:' <<< "$out" || { echo "FAILED: $name did not reach its ok: marker" >&2; exit 1; }
}
