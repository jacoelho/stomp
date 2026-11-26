import Stomp

/-!
`lake build` checks that each headline theorem depends on no axiom beyond
`propext`, `Classical.choice` and `Quot.sound`: in particular on no `sorryAx`.
-/

open Stomp Stomp.Session

/-- info: 'Stomp.unescape_escape' depends on axioms: [propext] -/
#guard_msgs in
#print axioms unescape_escape

/-- info: 'Stomp.escape_no_special' depends on axioms: [propext, Quot.sound] -/
#guard_msgs in
#print axioms escape_no_special

/-- info: 'Stomp.unescape_v11_cr_escape' depends on axioms: [propext] -/
#guard_msgs in
#print axioms unescape_v11_cr_escape

/-- info: 'Stomp.encode_ok_iff' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms encode_ok_iff

/-- info: 'Stomp.decode_encode' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms decode_encode

/-- info: 'Stomp.decode_encode_valid' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms decode_encode_valid

/-- info: 'Stomp.decodeFrame_heartbeats' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms decodeFrame_heartbeats

/-- info: 'Stomp.skipEOL_v11_cr' depends on axioms: [propext, Quot.sound] -/
#guard_msgs in
#print axioms skipEOL_v11_cr

/-- info: 'Stomp.negotiate_absent' depends on axioms: [propext] -/
#guard_msgs in
#print axioms negotiate_absent

/-- info: 'Stomp.negotiate_some_iff' depends on axioms: [propext] -/
#guard_msgs in
#print axioms negotiate_some_iff

/-- info: 'Stomp.negotiate_directions' depends on axioms: [propext, Quot.sound] -/
#guard_msgs in
#print axioms negotiate_directions

/-- info: 'Stomp.pick_eq_zero' depends on axioms: [propext] -/
#guard_msgs in
#print axioms pick_eq_zero

/-- info: 'Stomp.pick_eq_max' depends on axioms: [propext] -/
#guard_msgs in
#print axioms pick_eq_max

/-- info: 'Stomp.backoffCeil_eq_min' depends on axioms: [propext, Quot.sound] -/
#guard_msgs in
#print axioms backoffCeil_eq_min

/-- info: 'Stomp.Session.subscribe_in_use' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms subscribe_in_use

/-- info: 'Stomp.Session.unsubscribe_drains' depends on axioms: [propext] -/
#guard_msgs in
#print axioms unsubscribe_drains

/-- info: 'Stomp.Session.apply_draining' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms apply_draining

/-- info: 'Stomp.Session.receive_draining_discards' depends on axioms: [propext] -/
#guard_msgs in
#print axioms receive_draining_discards

/-- info: 'Stomp.Session.receive_unknown_discards' depends on axioms: [propext] -/
#guard_msgs in
#print axioms receive_unknown_discards

/-- info: 'Stomp.Session.receive_deliver' depends on axioms: [propext] -/
#guard_msgs in
#print axioms receive_deliver

/-- info: 'Stomp.Session.receive_missing_ack_id' depends on axioms: [propext] -/
#guard_msgs in
#print axioms receive_missing_ack_id

/-- info: 'Stomp.Session.ack_of_delivery' depends on axioms: [propext, Quot.sound] -/
#guard_msgs in
#print axioms ack_of_delivery

/-- info: 'Stomp.Session.receipts_issued_once_resolved_at_most_once' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms receipts_issued_once_resolved_at_most_once

/-- info: 'Stomp.Session.run_disconnecting' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms run_disconnecting

/-- info: 'Stomp.Session.disconnect_disconnecting' depends on axioms: [propext] -/
#guard_msgs in
#print axioms disconnect_disconnecting

/-- info: 'Stomp.Session.ack_stale' depends on axioms: [propext] -/
#guard_msgs in
#print axioms ack_stale

/-- info: 'Stomp.Session.stale_ack_builds_no_frame' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms stale_ack_builds_no_frame

/-- info: 'Stomp.Session.ack_earlier_subscription_stale' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms ack_earlier_subscription_stale

/-- info: 'Stomp.Session.reused_subscription_ack_builds_no_frame' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms reused_subscription_ack_builds_no_frame

/-- info: 'Stomp.Session.ackTx_stale' depends on axioms: [propext] -/
#guard_msgs in
#print axioms ackTx_stale

/-- info: 'Stomp.Session.checkTx_other_session' depends on axioms: [propext] -/
#guard_msgs in
#print axioms checkTx_other_session

/-- info: 'Stomp.Session.tx_other_session_builds_no_frame' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms tx_other_session_builds_no_frame

/-- info: 'Stomp.Session.checkTx_finished' depends on axioms: [propext] -/
#guard_msgs in
#print axioms checkTx_finished

/-- info: 'Stomp.Session.tx_finished_builds_no_frame' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms tx_finished_builds_no_frame

/-- info: 'Stomp.Session.finish_closes' depends on axioms: [propext] -/
#guard_msgs in
#print axioms finish_closes

/-- info: 'Stomp.Session.finished_tx_builds_no_frame' depends on axioms: [propext, Classical.choice, Quot.sound] -/
#guard_msgs in
#print axioms finished_tx_builds_no_frame

open Stomp.Lifecycle in
/-- info: 'Stomp.Lifecycle.step_life_forward' depends on axioms: [propext, Quot.sound] -/
#guard_msgs in
#print axioms step_life_forward

open Stomp.Lifecycle in
/-- info: 'Stomp.Lifecycle.connected_means_session' depends on axioms: [propext] -/
#guard_msgs in
#print axioms connected_means_session

open Stomp.Lifecycle in
/-- info: 'Stomp.Lifecycle.no_session_after_close' depends on axioms: [propext] -/
#guard_msgs in
#print axioms no_session_after_close

open Stomp.Lifecycle in
/-- info: 'Stomp.Lifecycle.state_frozen_after_close' depends on axioms: [propext] -/
#guard_msgs in
#print axioms state_frozen_after_close

open Stomp.Lifecycle in
/-- info: 'Stomp.Lifecycle.closed_is_final' depends on axioms: [propext] -/
#guard_msgs in
#print axioms closed_is_final
