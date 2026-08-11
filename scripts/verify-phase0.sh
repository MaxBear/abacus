#!/usr/bin/env bash
# Phase 0 acceptance checks, from the build plan's Verification section.
set -uo pipefail

pass=0; fail=0
ok()   { echo "  PASS  $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL  $1"; fail=$((fail+1)); }

echo "== Phase 0 verification =="

echo "-- liveness/readiness split"
code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8000/livez)
[ "$code" = "200" ] && ok "/livez 200" || bad "/livez returned $code"

code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8000/readyz)
[ "$code" = "200" ] && ok "/readyz 200 with deps up" || bad "/readyz returned $code"

echo "-- readyz fails when Postgres is down, livez does not"
docker compose stop postgres >/dev/null 2>&1
sleep 3
rcode=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8000/readyz)
hcode=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8000/livez)
[ "$rcode" = "503" ] && ok "/readyz 503 with postgres down" || bad "/readyz returned $rcode (want 503)"
[ "$hcode" = "200" ] && ok "/livez still 200 (no crash-loop)" || bad "/livez returned $hcode (want 200)"
docker compose start postgres >/dev/null 2>&1

echo "-- image hygiene"
img=$(docker compose config --images 2>/dev/null | grep -i abacus | head -1)
img=${img:-abacus-api}
user=$(docker inspect -f '{{.Config.User}}' "$img" 2>/dev/null)
[ "$user" = "app" ] && ok "runs as non-root ($user)" || bad "image user is '${user:-root}' (want app)"

entry=$(docker inspect -f '{{json .Config.Entrypoint}}' "$img" 2>/dev/null)
[[ "$entry" == *'"uvicorn"'* ]] && ok "exec-form ENTRYPOINT: $entry" || bad "ENTRYPOINT is $entry"

if docker history --no-trunc "$img" 2>/dev/null | grep -qiE '(password|secret|api[_-]?key)='; then
  bad "a secret-looking assignment appears in image history"
else
  ok "no secrets in image history"
fi

echo "-- SIGTERM reaches uvicorn (must return well under the 30s grace period)"
start=$(date +%s)
docker compose stop api >/dev/null 2>&1
elapsed=$(( $(date +%s) - start ))
[ "$elapsed" -lt 10 ] && ok "api stopped in ${elapsed}s" || bad "api took ${elapsed}s — SIGTERM likely not forwarded"
docker compose start api >/dev/null 2>&1

echo
echo "== $pass passed, $fail failed =="
[ "$fail" -eq 0 ]
