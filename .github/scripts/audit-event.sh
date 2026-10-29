#!/usr/bin/env bash
# Build one schemaVersion 2 audit event from the GitHub event payload (no secrets read, no network).
# Output: $OUT_DIR/event.v2.json (full v2 record) and $OUT_DIR/event.v1-wrap.json (hub v1 envelope,
# eventType evidence.record, carrying the v2 record until hub schemaVersion 2 is merged).
# Privacy: PR title and body are never stored as text; only their sha256 and length are recorded.
set -euo pipefail
OUT_DIR=${OUT_DIR:-audit-out}; mkdir -p "$OUT_DIR"
EV=${GITHUB_EVENT_NAME:?}; P=${GITHUB_EVENT_PATH:?}; KIND=${AUDIT_KIND:-}
ts=$(date -u +%Y-%m-%dT%H:%M:%S.000Z); ts_et=$(TZ=America/New_York date +"%Y-%m-%d %I:%M:%S %p ET")
case "$EV" in
  pull_request)
    act=$(jq -r .action "$P")
    case "$act" in closed) [ "$(jq -r .pull_request.merged "$P")" = true ] && type=pr.merged || type=pr.closed;;
      synchronize) type=pr.pushed;; opened|reopened|ready_for_review|converted_to_draft) type=pr.opened;; *) type=pr.opened;; esac
    key="$GITHUB_REPOSITORY|$(jq -r .pull_request.number "$P")|$act|$(jq -r .pull_request.head.sha "$P")";;
  pull_request_review) type=pr.reviewed; act=$(jq -r .review.state "$P"); key="$GITHUB_REPOSITORY|$(jq -r .pull_request.number "$P")|$(jq -r .review.id "$P")";;
  push) [[ "${GITHUB_REF}" == refs/tags/* ]] && type=tag.created || type=repo.push; act=push; key="$GITHUB_REPOSITORY|$GITHUB_REF|$(jq -r .after "$P")";;
  release) act=$(jq -r .action "$P"); case "$act" in published|created|released) type=release.published;; edited) type=release.edited;; deleted) type=release.deleted;; *) type=release.$act;; esac; key="$GITHUB_REPOSITORY|$(jq -r .release.id "$P")";;
  create|delete) rt=$(jq -r '.ref_type // "ref"' "$P"); [ "$EV" = create ] && type=$rt.created || type=$rt.deleted; act=$EV; key="$GITHUB_REPOSITORY|$EV|$(jq -r .ref "$P")|$GITHUB_RUN_ID";;
  workflow_run) type=ci.run; act=$(jq -r .workflow_run.conclusion "$P"); key="$GITHUB_REPOSITORY|$(jq -r .workflow_run.id "$P")|$(jq -r .workflow_run.run_attempt "$P")";;
  schedule|workflow_dispatch) type=${KIND:-drift.detected}; act=$EV; key="$GITHUB_REPOSITORY|$GITHUB_WORKFLOW|$GITHUB_RUN_ID|$GITHUB_RUN_ATTEMPT";;
  *) type=agent.action; act=$EV; key="$GITHUB_REPOSITORY|$EV|$GITHUB_RUN_ID";;
esac
[ -n "$KIND" ] && type=$KIND
tsha=$(jq -j '.pull_request.title // ""' "$P" | sha256sum | cut -c1-64)
bsha=$(jq -j '.pull_request.body // ""' "$P" | sha256sum | cut -c1-64)
eid=$(printf '%s|%s' "$type" "$key" | sha256sum | cut -c1-64)
body=$(jq -S -c -n --slurpfile e "$P" --arg type "$type" --arg eid "$eid" --arg ts "$ts" --arg tset "$ts_et" --arg act "$act" \
  --arg repo "$GITHUB_REPOSITORY" --arg actor "$GITHUB_ACTOR" --arg trig "${GITHUB_TRIGGERING_ACTOR:-$GITHUB_ACTOR}" \
  --arg run "$GITHUB_RUN_ID" --arg att "$GITHUB_RUN_ATTEMPT" --arg wf "$GITHUB_WORKFLOW" --arg sha "$GITHUB_SHA" --arg ref "$GITHUB_REF" \
  --arg extra "${AUDIT_EXTRA:-}" --arg tsha "$tsha" --arg bsha "$bsha" '
  ($e[0]) as $x | ($x.pull_request // {}) as $pr |
  ($pr.body // "") as $b | ($pr.title // "") as $t |
  {schemaVersion:2, event_id:$eid, event_type:$type, ts:$ts, ts_et:$tset, ts_source:"event",
   actor:{kind:(if ($actor|test("\\[bot\\]$")) then "bot" else "github_user" end), id:$actor, name:$actor},
   authorizer:null,
   correlation:{session:null, turn:null, parent:null, tool_call_id:null, source_ref:("gh-run:"+$run+"."+$att)},
   repo:$repo, pr:($pr.number // $x.workflow_run.pull_requests[0].number // null),
   head_sha:($pr.head.sha // $x.workflow_run.head_sha // $x.after // $sha), action:$act,
   target:($pr.base.ref // $x.workflow_run.head_branch // $ref), outcome:($x.review.state // $x.workflow_run.conclusion // $pr.state // "observed"),
   source:"github-actions",
   payload:{workflow:$wf, run_id:$run, attempt:$att, triggering_actor:$trig, ref:$ref,
     pr_title_sha256:(if $pr.title then $tsha else null end), pr_title_len:(if $pr.title then ($t|utf8bytelength) else null end),
     pr_body_sha256:(if $pr.body then $bsha else null end), pr_body_len:(if $pr.body then ($b|utf8bytelength) else null end), pr_author:($pr.user.login // null), draft:($pr.draft // null),
     requested_reviewers:[($pr.requested_reviewers // [])[].login], reviewer:($x.review.user.login // null), review_commit:($x.review.commit_id // null),
     merged_by:($pr.merged_by.login // null),
     before:($x.before // null), forced:($x.forced // null), commits:[($x.commits // [])[] | {id, author:.author.username, verified:(.verification.verified // null)}],
     workflow_run:(if $x.workflow_run then {name:$x.workflow_run.name, id:$x.workflow_run.id, event:$x.workflow_run.event, branch:$x.workflow_run.head_branch} else null end),
     extra:(if $extra=="" then {} else ($extra|fromjson) end)}}')
printf '%s\n' "$body" > "$OUT_DIR/event.v2.json"
env_name=${AUDIT_ENVIRONMENT:-merlin-stg}
jq -S -n --argjson v2 "$body" --arg repo "$GITHUB_REPOSITORY" --arg env "$env_name" \
 '{schemaVersion:1, sourceRepo:$repo, environment:$env, eventType:"evidence.record", attempt:1,
   actor:{kind:(if $v2.actor.kind=="bot" then "service" else "human" end), id:$v2.actor.id}, timestamp:$v2.ts,
   payload:{v2:$v2}}' > "$OUT_DIR/event.v1-wrap.json"
sha256sum "$OUT_DIR"/event.*.json > "$OUT_DIR/SHA256SUMS"
{ echo "### audit event"; echo "- type: \`$type\`"; echo "- event_id: \`$eid\`"; echo "- ts: $ts_et"; } >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
echo "audit-event $type $eid"
