#!/usr/bin/env bash
# Print globalId, contentId, and ccompat id for every version of a subject.
# Used to confirm which ID type the ccompat export gives Confluent.
#
# Usage:
#   bash scripts/show-ids.sh <subject>
set -euo pipefail

SUBJECT="${1:?usage: $0 <subject>}"
APICURIO_V2="${APICURIO_V2:-http://localhost:8081/apis/registry/v2}"
APICURIO_CC="${APICURIO_CC:-http://localhost:8081/apis/ccompat/v7}"
CONFLUENT_SR="${CONFLUENT_SR:-http://localhost:8082}"

printf "%-30s %-5s %-10s %-10s %-12s %-12s\n" \
  subject ver globalId contentId "ccompat-id" "confluent-id"
printf "%-30s %-5s %-10s %-10s %-12s %-12s\n" \
  "------------------------------" "-----" "--------" "---------" "----------" "-----------"

versions=$(curl -sf "$APICURIO_V2/groups/default/artifacts/$SUBJECT/versions" \
  | python3 -c "
import json, sys
data = json.load(sys.stdin)
vers = data.get('versions', data) if isinstance(data, dict) else data
for v in vers:
    if isinstance(v, dict):
        print(v.get('version',''), v.get('globalId',''), v.get('contentId',''))
    else:
        print(v, '', '')
" 2>/dev/null) || versions=""

if [[ -z "$versions" ]]; then
  echo "  (no versions found for subject '$SUBJECT')"
  exit 0
fi

while read -r ver gid cid; do
  [[ -z "$ver" ]] && continue
  ccompat_id=$(curl -sf "$APICURIO_CC/subjects/$SUBJECT/versions/$ver" \
    | python3 -c "import json,sys; print(json.load(sys.stdin).get('id','?'))" 2>/dev/null || echo "?")
  confluent_id=$(curl -sf "$CONFLUENT_SR/subjects/$SUBJECT/versions/$ver" \
    | python3 -c "import json,sys; print(json.load(sys.stdin).get('id','?'))" 2>/dev/null || echo "-")
  printf "%-30s %-5s %-10s %-10s %-12s %-12s\n" \
    "$SUBJECT" "$ver" "${gid:--}" "${cid:--}" "$ccompat_id" "$confluent_id"
done <<< "$versions"
