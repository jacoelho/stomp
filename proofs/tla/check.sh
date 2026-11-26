#!/bin/sh
# Model-checks StompClient.tla with TLC. Each run states its expected outcome:
# "pass", or the property TLC must report violated. Probes and mutants must
# fail, which shows the model reaches those cases and the properties can fail.
set -eu

cd "$(dirname "$0")"
jar=${TLA2TOOLS:-$HOME/.cache/tla/tla2tools.jar}
java=${JAVA:-java}
if ! "$java" -version >/dev/null 2>&1; then java=/opt/homebrew/opt/openjdk/bin/java; fi
[ -f "$jar" ] || { echo "tla2tools.jar not found at $jar; set TLA2TOOLS" >&2; exit 2; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failed=0
traces=${TRACES:-${TMPDIR:-/tmp}/stomp-tla-traces}
safety="INVARIANTS AckedWereReturned NoUnpermittedDiscard QueuedMeansConfirmed AcksNameOutstandingDeliveries NoFrameAfterDisconnect SendOkMeansAccepted NoMisroute ConnectedMeansSession NoSessionAfterClose ClosedHasNoSession
PROPERTIES LifeOnlyForward StateFrozenAfterClose"

# run NAME EXPECT SPEC CHECK AUTO MSGS SENDS WINDOW CLOSE CTX MUTANT [ENDONOVERFLOW [EARLYDELIVER]]
run() {
	name=$1 expect=$2 spec=$3 check=$4 auto=$5 msgs=$6 sends=$7 window=$8 close=$9
	shift 9
	ctx=$1 mutant=$2 endov=${3:-FALSE} early=${4:-FALSE}
	cat >"$work/$name.cfg" <<EOF
SPECIFICATION $spec
CONSTANTS
  Msgs = $msgs
  SendMsgs = $sends
  MaxGen = 2
  MaxInc = 2
  Cap = 1
  EndOnOverflow = $endov
  EarlyDeliver = $early
  Window = $window
  Auto = $auto
  EnableClose = $close
  CtxCancel = $ctx
  Mutant = "$mutant"
$check
EOF
	out=$("$java" -XX:+UseParallelGC -cp "$jar" tlc2.TLC -deadlock -noGenerateSpecTE -workers auto \
		-metadir "$work/$name.states" -config "$work/$name.cfg" StompClient.tla 2>&1) || true
	if echo "$out" | grep -q "No error has been found"; then
		got=pass
	elif v=$(echo "$out" | sed -n 's/^Error: Invariant \([A-Za-z]*\) is violated.*/\1/p' | head -1) && [ -n "$v" ]; then
		got=$v
	elif v=$(echo "$out" | sed -nE 's/^Error: (Temporal|Action) property ([A-Za-z]*) (was|is) violated.*/\2/p' | head -1) && [ -n "$v" ]; then
		got=$v
	else
		echo "$out" >&2
		got=error
	fi
	states=$(echo "$out" | sed -n 's/^\([0-9,]*\) states generated, \([0-9,]*\) distinct states found.*/\2 distinct states/p' | tail -1)
	if [ "$got" = "$expect" ]; then
		echo "ok   $name: $got ($states)"
	else
		echo "FAIL $name: got $got, want $expect ($states)"
		mkdir -p "$traces"
		echo "$out" | sed -n '/^Error:/,/^[0-9,]* states generated/p' >"$traces/$name.txt"
		echo "     trace in $traces/$name.txt"
		failed=1
	fi
}

two='{"m1", "m2"}'
one='{"m1"}'
s1='{"s1"}'
none='{}'

for auto in TRUE FALSE; do
	mode=$([ $auto = TRUE ] && echo auto || echo individual)
	run "safety-$mode" pass Spec "$safety" $auto "$one" "$s1" 2 TRUE TRUE none
	run "liveness-$mode" pass FairSpec "PROPERTY EveryMessageReturned" $auto "$two" "$none" 1 FALSE TRUE none
	run "probe-queued-across-reconnect-$mode" ProbeQueuedAcrossReconnect Spec \
		"INVARIANT ProbeQueuedAcrossReconnect" $auto "$one" "$none" 2 FALSE TRUE none
	run "probe-overflow-$mode" ProbeOverflow Spec "INVARIANT ProbeOverflow" $auto "$two" "$none" 2 FALSE TRUE none TRUE
	run "safety-end-on-overflow-$mode" pass Spec "$safety" $auto "$two" "$none" 2 FALSE TRUE none TRUE
	run "probe-append-after-loops-stopped-$mode" ProbeAppendAfterLoopsStopped Spec \
		"INVARIANT ProbeAppendAfterLoopsStopped" $auto "$one" "$s1" 1 FALSE TRUE none
	run "probe-resubscribed-holding-old-$mode" ProbeResubscribedHoldingOld Spec \
		"INVARIANT ProbeResubscribedHoldingOld" $auto "$one" "$none" 1 FALSE FALSE none
	run "mutant-nogen-$mode" AcksNameOutstandingDeliveries Spec "$safety" $auto "$one" "$none" 1 FALSE FALSE nogen
done
run mutant-noseq AcksNameOutstandingDeliveries Spec "$safety" TRUE "$one" "$none" 1 FALSE FALSE noseq
run mutant-ctxack EveryMessageReturned FairSpec "PROPERTY EveryMessageReturned" TRUE "$two" "$none" 1 FALSE FALSE ctxack
run mutant-ackonrecv AckedWereReturned Spec "$safety" TRUE "$one" "$none" 2 FALSE FALSE ackonrecv
run mutant-nowaiting ConnectedMeansSession Spec "$safety" TRUE "$one" "$none" 1 TRUE FALSE nowaiting
run mutant-overflowdefault NoUnpermittedDiscard Spec "$safety" FALSE "$two" "$none" 2 FALSE FALSE overflowdefault
run mutant-discardlive NoUnpermittedDiscard Spec "$safety" FALSE "$one" "$none" 1 FALSE FALSE discardlive
run safety-early-delivery pass Spec "$safety" FALSE "$one" "$none" 2 FALSE TRUE none FALSE TRUE
run probe-confirmed-by-message ProbeConfirmedByMessage Spec "INVARIANT ProbeConfirmedByMessage" FALSE "$one" "$none" 1 FALSE TRUE none FALSE TRUE
run mutant-dropconfirmed NoUnpermittedDiscard Spec "$safety" FALSE "$one" "$none" 1 FALSE FALSE dropconfirmed FALSE TRUE
run mutant-dialafterclose StateFrozenAfterClose Spec "$safety" TRUE "$one" "$none" 1 TRUE FALSE dialafterclose
run mutant-nobeginguard NoSessionAfterClose Spec "$safety" TRUE "$one" "$none" 1 TRUE FALSE nobeginguard

exit $failed
