import Stomp.Session
import Std.Data.String.ToNat

namespace Stomp.Session

/-! ## Per-call rules -/

/-- SUB-4: an id in the subscription table, active or draining its
UNSUBSCRIBE, cannot be subscribed again. -/
theorem subscribe_in_use {s : State} {id : String} (h : (s.subs id).isSome)
    (dest : String) (mode : Mode) (extra : Headers) :
    s.subscribe id dest mode extra =
      .error (if s.disconnecting then .disconnecting else .duplicateID) := by
  unfold State.subscribe
  cases s.disconnecting <;> simp [h]

/-- SUB-15, SUB-4: UNSUBSCRIBE leaves the id in the table, draining. -/
theorem unsubscribe_drains {s s' : State} {id : String} {out : OutFrame × String}
    (h : s.unsubscribe id = .ok (out, s')) :
    ∃ seq, s.subs id = some ⟨false, seq⟩ ∧ s'.subs id = some ⟨true, seq⟩ := by
  unfold State.unsubscribe at h
  split at h
  · simp at h
  · split at h
    · simp at h
    · next st hst =>
      split at h
      · simp at h
      · next hd =>
        simp at h
        obtain ⟨-, rfl⟩ := h
        refine ⟨st.seq, ?_, ?_⟩
        · cases st; simp_all
        · simp [State.issue, update]

/-- SUB-16: a MESSAGE for a draining subscription is discarded. -/
theorem receive_draining_discards {s : State} {f : Inbound} {sub : String} {st : Sub}
    (hc : f.command = .message) (hs : f.header "subscription" = some sub)
    (hst : s.subs sub = some st) (hd : st.draining = true) :
    s.receive f = .ok (.discard sub, s) := by
  simp [State.receive, hc, hs, hst, hd]

/-- SUB-2: a MESSAGE for a subscription the session does not hold is discarded,
and the session goes on. -/
theorem receive_unknown_discards {s : State} {f : Inbound} {sub : String}
    (hc : f.command = .message) (hs : f.header "subscription" = some sub)
    (hn : s.subs sub = none) :
    s.receive f = .ok (.discard sub, s) := by
  simp [State.receive, hc, hs, hn]

/-- SUB-16, SUB-23, SUB-27: a delivery names a subscription that is present and
not draining, leaves the session unchanged, and carries the value of the
version's acknowledgement header (`ack` in 1.2, `message-id` in 1.1) and the
subscription's sequence number (SUB-31). -/
theorem receive_deliver {s s' : State} {f : Inbound} {sub ackId : String} {seq : Nat}
    (h : s.receive f = .ok (.deliver sub seq ackId, s')) :
    s' = s ∧ f.command = .message ∧ f.header "subscription" = some sub ∧
      ∃ st, s.subs sub = some st ∧ st.draining = false ∧ st.seq = seq ∧
        f.header (ackIdHeader s.ver) = some ackId := by
  unfold State.receive at h
  split at h
  · next hc =>
    split at h
    · simp at h
    · next id hid =>
      split at h
      · simp at h
      · next st hst =>
        split at h
        · simp at h
        · next hd =>
          split at h
          · simp at h
          · next a ha =>
            simp at h
            obtain ⟨⟨rfl, rfl, rfl⟩, rfl⟩ := h
            exact ⟨rfl, hc, hid, st, hst, by simpa using hd, rfl, ha⟩
  · dsimp only at h; split at h <;> simp at h
  · dsimp only at h; split at h <;> simp at h
  · simp at h

/-- SUB-23: a MESSAGE for an active subscription without the version's
acknowledgement header ends the session with a protocol error. -/
theorem receive_missing_ack_id {s : State} {f : Inbound} {sub : String} {st : Sub}
    (hc : f.command = .message) (hs : f.header "subscription" = some sub)
    (hst : s.subs sub = some st) (hd : st.draining = false)
    (ha : f.header (ackIdHeader s.ver) = none) :
    s.receive f = .error .protocol := by
  simp [State.receive, hc, hs, hst, hd, ha]

/-- The acknowledgement headers of version `v` (SUB-27). -/
def ackIdHeaders (v : Version) (sub ackId : String) : Headers :=
  match v with
  | .v11 => [("message-id", ackId), ("subscription", sub)]
  | .v12 => [("id", ackId)]

/-- SUB-23, SUB-27: acknowledging a delivery of this session builds an ACK or
NACK carrying the delivered acknowledgement id under the version's header
names, followed by the receipt when one is requested. `hdisc` is needed
because a disconnecting session builds no ACK (CON-25). -/
theorem ack_of_delivery {s s' : State} {f : Inbound} {sub ackId : String} {seq : Nat}
    (hdisc : s.disconnecting = false)
    (h : s.receive f = .ok (.deliver sub seq ackId, s'))
    (nack wait : Bool) :
    ∃ r s'', s'.ack ⟨s.gen, sub, seq, ackId⟩ nack wait =
      .ok ((⟨ackCommand nack, ackIdHeaders s.ver sub ackId ++ (if wait then [("receipt", r)] else []), []⟩, r), s'') := by
  obtain ⟨rfl, -, -, st, hst, hd, hq, -⟩ := receive_deliver h
  cases hv : s'.ver <;> cases wait <;>
    simp [State.ack, ackHeaders, hdisc, hst, hd, hq, hv, ackIdHeaders, State.issue]

/-- SUB-21: an acknowledgement of a message from another session fails and
builds no frame. -/
theorem ack_stale {s : State} {h : AckHandle} (hg : h.gen ≠ s.gen) (nack wait : Bool) :
    s.ack h nack wait = .error (if s.disconnecting then .disconnecting else .staleAck) := by
  cases hd : s.disconnecting <;> simp [State.ack, ackHeaders, hd, hg]

/-- TX-5: a transaction from another session builds no frame. -/
theorem checkTx_other_session {s : State} {tx : TxHandle} (hg : tx.gen ≠ s.gen) :
    checkTx s tx = .error (if s.disconnecting then .disconnecting else .txAborted) := by
  cases hd : s.disconnecting <;> simp [checkTx, hd, hg]

/-- TX-7: a handle of this session whose `Begin` no longer holds its id builds
no frame. -/
theorem checkTx_finished {s : State} {tx : TxHandle} (hg : tx.gen = s.gen) (ho : s.txs tx.id ≠ some tx.seq) :
    checkTx s tx = .error (if s.disconnecting then .disconnecting else .txDone) := by
  cases hd : s.disconnecting <;> simp [checkTx, hd, hg, ho]

theorem sendTx_of_checkTx {s : State} {tx : TxHandle} {e : Error} (h : checkTx s tx = .error e) (m : Outgoing) :
    s.sendTx tx m = .error e := by
  simp [State.sendTx, h]

theorem ackTx_of_checkTx {s : State} {tx : TxHandle} {e : Error} (h : checkTx s tx = .error e)
    (a : AckHandle) (nack : Bool) : s.ackTx tx a nack = .error e := by
  simp [State.ackTx, h]

theorem finish_of_checkTx {s : State} {tx : TxHandle} {e : Error} (h : checkTx s tx = .error e) (abort : Bool) :
    s.finish tx abort = .error e := by
  simp [State.finish, h]

/-- TX-10: an acknowledgement in an open transaction of a message from another
session fails with `ErrStaleAck`. -/
theorem ackTx_stale {s : State} {tx : TxHandle} {a : AckHandle} (ht : checkTx s tx = .ok ())
    (hg : a.gen ≠ s.gen) (nack : Bool) : s.ackTx tx a nack = .error .staleAck := by
  have hd : s.disconnecting = false := by
    unfold checkTx at ht; cases h : s.disconnecting <;> simp_all
  simp [State.ackTx, ht, ackHeaders, hd, hg]

/-- TX-7: COMMIT or ABORT closes the transaction. -/
theorem finish_closes {s s' : State} {tx : TxHandle} {abort : Bool} {out : OutFrame × String}
    (h : s.finish tx abort = .ok (out, s')) : s'.txs tx.id = none := by
  unfold State.finish at h
  split at h
  · simp at h
  · simp at h; obtain ⟨-, rfl⟩ := h; simp [State.issue, update]

/-- The transaction a call names, for the calls that need an open one. -/
def Op.txHandle : Op → Option TxHandle
  | .ackTx tx _ _ => some tx
  | .sendTx tx _ => some tx
  | .finish tx _ => some tx
  | _ => none

/-- TX-5: a SEND, ACK, NACK, COMMIT or ABORT in a transaction of another
session fails with `ErrTxAborted` (or `ErrDisconnecting`) and builds no frame. -/
theorem tx_other_session_builds_no_frame {s : State} {op : Op} {tx : TxHandle}
    (hop : op.txHandle = some tx) (hg : tx.gen ≠ s.gen) :
    s.apply op = .error (if s.disconnecting then .disconnecting else .txAborted) := by
  have he := checkTx_other_session (s := s) hg
  cases op <;> simp [Op.txHandle] at hop <;> subst hop <;>
    simp [State.apply, State.ackTx, State.sendTx, State.finish, he, Except.map] <;> rfl

/-- TX-7: a SEND, ACK, NACK, COMMIT or ABORT on a handle of this session whose
`Begin` no longer holds its id fails with `ErrTxDone` (or `ErrDisconnecting`)
and builds no frame. -/
theorem tx_finished_builds_no_frame {s : State} {op : Op} {tx : TxHandle}
    (hop : op.txHandle = some tx) (hg : tx.gen = s.gen) (ho : s.txs tx.id ≠ some tx.seq) :
    s.apply op = .error (if s.disconnecting then .disconnecting else .txDone) := by
  have he := checkTx_finished (s := s) hg ho
  cases op <;> simp [Op.txHandle] at hop <;> subst hop <;>
    simp [State.apply, State.ackTx, State.sendTx, State.finish, he, Except.map] <;> rfl

/-- SUB-21, TX-10: acknowledging a message from another session, with or
without a transaction, fails and builds no frame. -/
theorem stale_ack_builds_no_frame {s : State} {a : AckHandle} (hg : a.gen ≠ s.gen) (nack : Bool) :
    (∀ wait, ∃ e, s.apply (.ack a nack wait) = .error e) ∧
      (∀ tx, ∃ e, s.apply (.ackTx tx a nack) = .error e) := by
  refine ⟨fun wait => ⟨_, by simp [State.apply, ack_stale hg, Except.map]; rfl⟩, fun tx => ?_⟩
  cases ht : checkTx s tx with
  | error e => exact ⟨e, by simp [State.apply, ackTx_of_checkTx ht, Except.map]; rfl⟩
  | ok u => exact ⟨.staleAck, by cases u; simp [State.apply, ackTx_stale ht hg, Except.map]; rfl⟩

/-- SUB-31: acknowledging a delivery whose subscription is not the one now
holding its id fails with `ErrStaleAck` (or `ErrDisconnecting`), with or
without a transaction, and builds no frame. -/
theorem ack_earlier_subscription_stale {s : State} {a : AckHandle} {st : Sub}
    (hs : s.subs a.sub = some st) (hq : st.seq ≠ a.seq) (nack : Bool) :
    (∀ wait, s.apply (.ack a nack wait) = .error (if s.disconnecting then .disconnecting else .staleAck)) ∧
      (∀ tx, checkTx s tx = .ok () → s.apply (.ackTx tx a nack) = .error .staleAck) := by
  have hh : ackHeaders s a = .error (if s.disconnecting then .disconnecting else .staleAck) := by
    unfold ackHeaders
    cases s.disconnecting <;> by_cases hg : a.gen = s.gen <;> simp [hg, hs, hq]
  refine ⟨fun wait => by simp [State.apply, State.ack, hh, Except.map], fun tx ht => ?_⟩
  have hd : s.disconnecting = false := by
    unfold checkTx at ht; cases h : s.disconnecting <;> simp_all
  simp [State.apply, State.ackTx, ht, hh, hd, Except.map]; rfl

/-! ## Effects of one call -/

theorem apply_gen {s s' : State} {op : Op} {out : Output} (h : s.apply op = .ok (out, s')) :
    s'.gen = s.gen := by
  cases op <;> simp only [State.apply, State.subscribe, State.unsubscribe, State.ack, State.ackTx,
    State.send, State.sendTx, State.begin, State.finish, State.disconnect, State.endSub,
    State.receive, State.issue] at h <;> (repeat' split at h) <;> simp [Except.map] at h <;>
    (try obtain ⟨-, rfl⟩ := h) <;> rfl

/-- The receipt id a call requested, if any. -/
def Output.issued (o : Output) : Option String := if o.receipt = "" then none else some o.receipt

/-- The pending receipt a call resolved: a RECEIPT, or an ERROR whose
`receipt-id` matched. -/
def Output.resolved (o : Output) : Option String :=
  match o.event with
  | some (.receipt id _) => some id
  | some (.error id (some _)) => some id
  | _ => none

/-- A call leaves the receipt table alone, issues the next receipt id, or
resolves one pending receipt. -/
theorem apply_receipts {s s' : State} {op : Op} {out : Output} (h : s.apply op = .ok (out, s')) :
    (s'.next = s.next ∧ s'.receipts = s.receipts ∧ out.receipt = "" ∧ out.resolved = none) ∨
    (s'.next = s.next + 1 ∧ (∃ p, s'.receipts = update s.receipts (receiptId s.gen (s.next + 1)) (some p)) ∧
      out.receipt = receiptId s.gen (s.next + 1) ∧ out.resolved = none) ∨
    (∃ id, (s.receipts id).isSome ∧ s'.next = s.next ∧ s'.receipts = update s.receipts id none ∧
      out.receipt = "" ∧ out.resolved = some id) := by
  cases op <;> simp only [State.apply, State.subscribe, State.unsubscribe, State.ack, State.ackTx,
    State.send, State.sendTx, State.begin, State.finish, State.disconnect, State.endSub,
    State.receive, State.issue] at h <;> (repeat' split at h) <;> simp [Except.map] at h <;>
    (try obtain ⟨rfl, rfl⟩ := h) <;>
    first
    | exact .inl ⟨rfl, rfl, rfl, rfl⟩
    | exact .inr (.inl ⟨rfl, ⟨_, rfl⟩, rfl, rfl⟩)
    | exact .inr (.inr ⟨_, by simp [*], rfl, rfl, rfl, rfl⟩)

/-! ## No frame after DISCONNECT (CON-25) -/

theorem apply_disconnecting {s s' : State} {op : Op} {out : Output} (h : s.apply op = .ok (out, s'))
    (hd : s.disconnecting = true) : s'.disconnecting = true ∧ out.frame = none := by
  cases op <;> simp only [State.apply, State.subscribe, State.unsubscribe, State.ack, State.ackTx,
    State.send, State.sendTx, State.begin, State.finish, State.disconnect, State.endSub,
    State.receive, State.issue, ackHeaders, checkTx] at h <;> (repeat' split at h) <;>
    simp [Except.map] at h <;> (try obtain ⟨rfl, rfl⟩ := h) <;> simp_all

/-- CON-25: once the session is disconnecting, no call builds a frame, a second
`Disconnect` included, and the session stays disconnecting. Each pair is a call
and its result. -/
theorem run_disconnecting {s : State} (hd : s.disconnecting = true) (ops : List Op) :
    ∀ p ∈ ops.zip (s.run ops).1, ∀ out, p.2 = .ok out → out.frame = none := by
  induction ops generalizing s with
  | nil => simp [State.run]
  | cons op ops ih =>
    simp only [State.run]
    split
    · intro p hp
      simp only [List.zip_cons_cons, List.mem_cons] at hp
      rcases hp with rfl | hp
      · simp
      · exact ih hd p hp
    · next out s1 h =>
      intro p hp
      simp only [List.zip_cons_cons, List.mem_cons] at hp
      rcases hp with rfl | hp
      · intro o ho; simp at ho; subst ho; exact (apply_disconnecting h hd).2
      · exact ih (apply_disconnecting h hd).1 p hp

/-- CON-25: a successful `Disconnect` marks the session disconnecting. -/
theorem disconnect_disconnecting {s s' : State} {out : OutFrame × String}
    (h : s.disconnect = .ok (out, s')) : s'.disconnecting = true := by
  unfold State.disconnect at h
  split at h
  · simp at h
  · simp at h; obtain ⟨-, rfl⟩ := h; rfl

/-! ## Finished transactions (TX-7) -/

/-- A call leaves the transaction table alone, closes one id, or opens a free
id under the next sequence number. -/
theorem apply_txs {s s' : State} {op : Op} {out : Output} (h : s.apply op = .ok (out, s')) :
    (s'.txs = s.txs ∧ s'.nextTx = s.nextTx) ∨
    (∃ id, s'.txs = update s.txs id none ∧ s'.nextTx = s.nextTx) ∨
    (∃ id, s'.txs = update s.txs id (some (s.nextTx + 1)) ∧ s'.nextTx = s.nextTx + 1) := by
  cases op <;> simp only [State.apply, State.subscribe, State.unsubscribe, State.ack, State.ackTx,
    State.send, State.sendTx, State.begin, State.finish, State.disconnect, State.endSub,
    State.receive, State.issue] at h <;> (repeat' split at h) <;> simp [Except.map] at h <;>
    (try obtain ⟨rfl, rfl⟩ := h) <;>
    first
    | exact .inl ⟨rfl, rfl⟩
    | exact .inr (.inl ⟨_, rfl, rfl⟩)
    | exact .inr (.inr ⟨_, rfl, rfl⟩)

/-- Every open transaction's sequence number has been issued: it is at most
`nextTx`. -/
def TxSeqBound (s : State) : Prop := ∀ id k, s.txs id = some k → k ≤ s.nextTx

theorem txSeqBound_apply {s s' : State} {op : Op} {out : Output} (h : s.apply op = .ok (out, s'))
    (hb : TxSeqBound s) : TxSeqBound s' := by
  intro id k hk
  rcases apply_txs h with ⟨ht, hn⟩ | ⟨i, ht, hn⟩ | ⟨i, ht, hn⟩
  · rw [ht] at hk; rw [hn]; exact hb id k hk
  · rw [ht] at hk; rw [hn]; simp only [update] at hk
    split at hk
    · simp at hk
    · exact hb id k hk
  · rw [ht] at hk; rw [hn]; simp only [update] at hk
    split at hk
    · simp at hk; omega
    · have := hb id k hk; omega

theorem txSeqBound_run {s : State} (hb : TxSeqBound s) (ops : List Op) : TxSeqBound (s.run ops).2 := by
  induction ops generalizing s with
  | nil => exact hb
  | cons op ops ih =>
    simp only [State.run]
    split
    · exact ih hb
    · next out s1 h => exact ih (txSeqBound_apply h hb)

/-- A handle is dead when its `Begin` no longer holds its id and every later
`Begin` takes a larger sequence number. -/
def Dead (tx : TxHandle) (s : State) : Prop := s.txs tx.id ≠ some tx.seq ∧ tx.seq ≤ s.nextTx

theorem dead_apply {s s' : State} {op : Op} {out : Output} {tx : TxHandle}
    (h : s.apply op = .ok (out, s')) (hd : Dead tx s) : Dead tx s' := by
  obtain ⟨hne, hle⟩ := hd
  rcases apply_txs h with ⟨ht, hn⟩ | ⟨i, ht, hn⟩ | ⟨i, ht, hn⟩
  · exact ⟨by rw [ht]; exact hne, by rw [hn]; exact hle⟩
  · refine ⟨?_, by rw [hn]; exact hle⟩
    rw [ht]; simp only [update]; split
    · simp
    · exact hne
  · refine ⟨?_, by rw [hn]; omega⟩
    rw [ht]; simp only [update]; split
    · simp; omega
    · exact hne

theorem apply_dead_fails {s : State} {op : Op} {tx : TxHandle} (hop : op.txHandle = some tx)
    (hc : s.txs tx.id ≠ some tx.seq) : ∃ e, s.apply op = .error e := by
  have hchk : ∃ e, checkTx s tx = .error e := by
    unfold checkTx; split
    · exact ⟨_, rfl⟩
    · split
      · exact ⟨_, rfl⟩
      · simp
  obtain ⟨e, he⟩ := hchk
  cases op <;> simp [Op.txHandle] at hop <;> subst hop <;>
    simp [State.apply, State.ackTx, State.sendTx, State.finish, he, Except.map] <;> exact ⟨e, rfl⟩

theorem run_dead {s : State} {tx : TxHandle} (hd : Dead tx s) (ops : List Op) :
    ∀ p ∈ ops.zip (s.run ops).1, p.1.txHandle = some tx → ∃ e, p.2 = .error e := by
  induction ops generalizing s with
  | nil => simp [State.run]
  | cons op ops ih =>
    simp only [State.run]
    split
    · next e h =>
      intro p hp
      simp only [List.zip_cons_cons, List.mem_cons] at hp
      rcases hp with rfl | hp
      · intro _; exact ⟨e, rfl⟩
      · exact ih hd p hp
    · next out s2 h =>
      intro p hp
      simp only [List.zip_cons_cons, List.mem_cons] at hp
      rcases hp with rfl | hp
      · intro ht
        obtain ⟨e, he⟩ := apply_dead_fails ht hd.1
        rw [he] at h; simp at h
      · exact ih (dead_apply h hd) p hp

/-- TX-7: in any session reached from `New`, once COMMIT or ABORT succeeds on a
handle, every later SEND, ACK, NACK, COMMIT or ABORT on that handle fails and
builds no frame, on any trace, even one whose `Begin` reuses the id. -/
theorem finished_tx_builds_no_frame (g : Nat) (v : Version) (pre : List Op) {tx : TxHandle}
    {abort : Bool} {fo : OutFrame × String} {s1 : State}
    (hf : ((new g v).run pre).2.finish tx abort = .ok (fo, s1)) (ops : List Op) :
    ∀ p ∈ ops.zip (s1.run ops).1, p.1.txHandle = some tx → ∃ e, p.2 = .error e := by
  apply run_dead _ ops
  have hb := txSeqBound_run (s := new g v) (fun _ _ h => by simp [new] at h) pre
  generalize ((new g v).run pre).2 = s at hf hb
  unfold State.finish at hf
  split at hf
  · simp at hf
  · next hchk =>
    simp at hf; obtain ⟨-, rfl⟩ := hf
    have hopen : s.txs tx.id = some tx.seq := by
      unfold checkTx at hchk
      split at hchk; · simp at hchk
      split at hchk; · simp at hchk
      split at hchk
      · simp at hchk
      · next h => exact Classical.not_not.mp h
    exact ⟨by simp [State.issue, update], by simpa [State.issue] using hb _ _ hopen⟩

/-! ## Draining subscriptions (SUB-4, SUB-16) -/

/-- A draining subscription stays in the table, draining, until the RECEIPT of
its UNSUBSCRIBE: so by `subscribe_in_use` its id cannot be reused and by
`receive_draining_discards` none of its MESSAGE frames is delivered. -/
theorem apply_draining {s s' : State} {op : Op} {out : Output} {id : String} {seq : Nat}
    (hs : s.subs id = some ⟨true, seq⟩) (h : s.apply op = .ok (out, s')) :
    s'.subs id = some ⟨true, seq⟩ ∨
      ∃ r p, out.event = some (.receipt r p) ∧ p.kind = .unsubscribe ∧ p.sub = id := by
  cases op <;> simp only [State.apply, State.subscribe, State.unsubscribe, State.ack, State.ackTx,
    State.send, State.sendTx, State.begin, State.finish, State.disconnect, State.endSub,
    State.receive, State.issue] at h <;> (repeat' split at h) <;>
    simp [Except.map] at h <;> (try obtain ⟨rfl, rfl⟩ := h) <;> (try simp_all [update]) <;>
    (try ((repeat' split) <;> simp_all)) <;>
    first
    | (intro he; subst he; simp_all)
    | exact .inr ⟨_, _, ⟨rfl, rfl⟩, ‹_›, rfl⟩
    | exact .inr ⟨_, _, ⟨rfl, rfl⟩, ‹_›, Eq.symm ‹_›⟩
    | exact (Classical.em _).elim (fun he => .inr ⟨_, _, ⟨rfl, rfl⟩, ‹_›, Eq.symm he⟩) .inl

/-! ## Reused subscription ids (SUB-31) -/

/-- A call leaves every subscription's sequence number alone and keeps
`nextSub`, or registers one id under the next sequence number. -/
theorem apply_subs {s s' : State} {op : Op} {out : Output} (h : s.apply op = .ok (out, s')) :
    (s'.nextSub = s.nextSub ∧ ∀ id st, s'.subs id = some st → ∃ st0, s.subs id = some st0 ∧ st0.seq = st.seq) ∨
    (∃ id, s'.nextSub = s.nextSub + 1 ∧ s'.subs = update s.subs id (some ⟨false, s.nextSub + 1⟩)) := by
  cases op <;> simp only [State.apply, State.subscribe, State.unsubscribe, State.ack, State.ackTx,
    State.send, State.sendTx, State.begin, State.finish, State.disconnect, State.endSub,
    State.receive, State.issue] at h <;> (repeat' split at h) <;> simp [Except.map] at h <;>
    (try obtain ⟨-, rfl⟩ := h) <;>
    first
    | exact .inr ⟨_, rfl, rfl⟩
    | exact .inl ⟨rfl, fun id st h => ⟨st, h, rfl⟩⟩
    | refine .inl ⟨rfl, fun id st h => ?_⟩
      simp only [update] at h
      split at h
      · next he => subst he; simp at h <;> exact ⟨_, ‹_›, by rw [← h]⟩
      · exact ⟨st, h, rfl⟩

/-- Every subscription's sequence number has been issued: it is at most
`nextSub`. -/
def SubSeqBound (s : State) : Prop := ∀ id st, s.subs id = some st → st.seq ≤ s.nextSub

theorem subSeqBound_apply {s s' : State} {op : Op} {out : Output} (h : s.apply op = .ok (out, s'))
    (hb : SubSeqBound s) : SubSeqBound s' := by
  intro id st hst
  rcases apply_subs h with ⟨hn, hk⟩ | ⟨i, hn, hs⟩
  · obtain ⟨st0, h0, hq⟩ := hk id st hst
    rw [hn, ← hq]; exact hb id st0 h0
  · rw [hs] at hst; rw [hn]; simp only [update] at hst
    split at hst
    · simp at hst; subst hst; simp
    · have := hb id st hst; omega

theorem apply_nextSub_le {s s' : State} {op : Op} {out : Output} (h : s.apply op = .ok (out, s')) :
    s.nextSub ≤ s'.nextSub := by
  rcases apply_subs h with ⟨hn, -⟩ | ⟨-, hn, -⟩ <;> omega

theorem run_subSeqBound {s : State} (hb : SubSeqBound s) (ops : List Op) :
    SubSeqBound (s.run ops).2 ∧ s.nextSub ≤ (s.run ops).2.nextSub := by
  induction ops generalizing s with
  | nil => exact ⟨hb, Nat.le_refl _⟩
  | cons op ops ih =>
    simp only [State.run]
    split
    · exact ih hb
    · next out s1 h =>
      have := ih (subSeqBound_apply h hb)
      exact ⟨this.1, Nat.le_trans (apply_nextSub_le h) this.2⟩

/-- An acknowledgement handle is outdated when its sequence number has been
issued and the subscription holding its id, if any, has another. -/
def Outdated (a : AckHandle) (s : State) : Prop :=
  a.seq ≤ s.nextSub ∧ ∀ st, s.subs a.sub = some st → st.seq ≠ a.seq

theorem outdated_apply {s s' : State} {op : Op} {out : Output} {a : AckHandle}
    (h : s.apply op = .ok (out, s')) (ho : Outdated a s) : Outdated a s' := by
  obtain ⟨hle, hne⟩ := ho
  rcases apply_subs h with ⟨hn, hk⟩ | ⟨i, hn, hs⟩
  · refine ⟨by rw [hn]; exact hle, fun st hst => ?_⟩
    obtain ⟨st0, h0, hq⟩ := hk _ st hst
    rw [← hq]; exact hne st0 h0
  · refine ⟨by rw [hn]; omega, fun st hst => ?_⟩
    rw [hs] at hst; simp only [update] at hst
    split at hst
    · simp at hst; subst hst; simp; omega
    · exact hne st hst

/-- The acknowledgement a call makes, for the calls that make one. -/
def Op.ackHandle : Op → Option AckHandle
  | .ack a _ _ => some a
  | .ackTx _ a _ => some a
  | _ => none

theorem apply_outdated_fails {s : State} {op : Op} {a : AckHandle} (hop : op.ackHandle = some a)
    (ho : Outdated a s) : ∃ e, s.apply op = .error e := by
  have hh : ∃ e, ackHeaders s a = .error e := by
    unfold ackHeaders; split
    · exact ⟨_, rfl⟩
    · split
      · exact ⟨_, rfl⟩
      · split
        · exact ⟨_, rfl⟩
        · next st hst => simp [ho.2 st hst]
  obtain ⟨e, he⟩ := hh
  cases op with
  | ack a' nack wait =>
    simp [Op.ackHandle] at hop; subst hop
    exact ⟨e, by simp [State.apply, State.ack, he, Except.map]⟩
  | ackTx tx a' nack =>
    simp [Op.ackHandle] at hop; subst hop
    cases ht : checkTx s tx with
    | error e' => exact ⟨e', by simp [State.apply, State.ackTx, ht, Except.map]; rfl⟩
    | ok u => exact ⟨e, by cases u; simp [State.apply, State.ackTx, ht, he, Except.map]; rfl⟩
  | _ => simp [Op.ackHandle] at hop

theorem run_outdated {s : State} {a : AckHandle} (ho : Outdated a s) (ops : List Op) :
    ∀ p ∈ ops.zip (s.run ops).1, p.1.ackHandle = some a → ∃ e, p.2 = .error e := by
  induction ops generalizing s with
  | nil => simp [State.run]
  | cons op ops ih =>
    simp only [State.run]
    split
    · next e h =>
      intro p hp
      simp only [List.zip_cons_cons, List.mem_cons] at hp
      rcases hp with rfl | hp
      · intro _; exact ⟨e, rfl⟩
      · exact ih ho p hp
    · next out s2 h =>
      intro p hp
      simp only [List.zip_cons_cons, List.mem_cons] at hp
      rcases hp with rfl | hp
      · intro ha
        obtain ⟨e, he⟩ := apply_outdated_fails ha ho
        rw [he] at h; simp at h
      · exact ih (outdated_apply h ho) p hp

/-- SUB-31: in any session reached from `New`, once a subscription has
delivered a message and its id has been subscribed again, every later ACK or
NACK of that delivery, with or without a transaction, fails and builds no
frame, on any trace. -/
theorem reused_subscription_ack_builds_no_frame (g : Nat) (v : Version) (pre mid : List Op)
    {f : Inbound} {a : AckHandle} {s0 : State}
    (hd : ((new g v).run pre).2.receive f = .ok (.deliver a.sub a.seq a.id, s0))
    {dest : String} {mode : Mode} {extra : Headers} {out : OutFrame × String} {s1 : State}
    (hs : (((new g v).run pre).2.run mid).2.subscribe a.sub dest mode extra = .ok (out, s1))
    (ops : List Op) :
    ∀ p ∈ ops.zip (s1.run ops).1, p.1.ackHandle = some a → ∃ e, p.2 = .error e := by
  apply run_outdated _ ops
  have hb0 : SubSeqBound (new g v) := fun _ _ h => by simp [new] at h
  obtain ⟨hb, -⟩ := run_subSeqBound hb0 pre
  obtain ⟨-, hmono⟩ := run_subSeqBound hb mid
  obtain ⟨-, -, -, st, hst, -, hq, -⟩ := receive_deliver hd
  have hle : a.seq ≤ (((new g v).run pre).2.run mid).2.nextSub := hq ▸ Nat.le_trans (hb _ _ hst) hmono
  generalize (((new g v).run pre).2.run mid).2 = s at hs hle
  unfold State.subscribe at hs
  split at hs
  · simp at hs
  · split at hs
    · simp at hs
    · split at hs
      · simp at hs
      · simp [State.issue] at hs; obtain ⟨-, rfl⟩ := hs
        exact ⟨by simp; omega, fun st h => by simp [update] at h; subst h; simp; omega⟩

/-! ## Receipts over a trace (SND-5, SUB-15) -/

theorem receiptId_inj {g n m : Nat} (h : receiptId g n = receiptId g m) : n = m := by
  unfold receiptId at h
  have := congrArg String.toList h
  simp only [String.toList_append] at this
  exact Nat.repr_inj.mp (String.toList_inj.mp (List.append_cancel_left this))

theorem receiptId_ne_empty (g n : Nat) : receiptId g n ≠ "" := by
  unfold receiptId; intro h
  have := congrArg String.length h
  simp at this

def issuedIds (outs : List (Except Error Output)) : List String :=
  outs.filterMap fun | .ok o => o.issued | .error _ => none

def resolvedIds (outs : List (Except Error Output)) : List String :=
  outs.filterMap fun | .ok o => o.resolved | .error _ => none

theorem issuedIds_ok (o : Output) (outs : List (Except Error Output)) :
    issuedIds (.ok o :: outs) = o.issued.toList ++ issuedIds outs := by
  cases h : o.issued <;> simp [issuedIds, h]

theorem resolvedIds_ok (o : Output) (outs : List (Except Error Output)) :
    resolvedIds (.ok o :: outs) = o.resolved.toList ++ resolvedIds outs := by
  cases h : o.resolved <;> simp [resolvedIds, h]

/-- The receipt ids issued so far are `<gen>-1 … <gen>-next`; pending and
resolved ids are among them, and a resolved id is no longer pending. -/
structure ReceiptInv (s : State) (issued resolved : List String) : Prop where
  issued_eq : issued = (List.range s.next).map fun k => receiptId s.gen (k + 1)
  pending_issued : ∀ r, (s.receipts r).isSome → r ∈ issued
  resolved_done : ∀ r ∈ resolved, s.receipts r = none ∧ r ∈ issued
  resolved_nodup : resolved.Nodup

theorem receiptInv_new (g : Nat) (v : Version) : ReceiptInv (new g v) [] [] :=
  ⟨rfl, by simp [new], by simp, .nil⟩

theorem receiptInv_apply {s s' : State} {op : Op} {out : Output} {issued resolved : List String}
    (inv : ReceiptInv s issued resolved) (h : s.apply op = .ok (out, s')) :
    ReceiptInv s' (issued ++ out.issued.toList) (resolved ++ out.resolved.toList) := by
  have hg := apply_gen h
  rcases apply_receipts h with ⟨hn, hr, hrc, hres⟩ | ⟨hn, ⟨p, hr⟩, hrc, hres⟩ | ⟨id, hid, hn, hr, hrc, hres⟩
  · have hi : out.issued = none := by simp [Output.issued, hrc]
    rw [hi, hres]
    simp only [Option.toList_none, List.append_nil]
    exact ⟨by rw [hn, hg]; exact inv.issued_eq, by rw [hr]; exact inv.pending_issued,
      by rw [hr]; exact inv.resolved_done, inv.resolved_nodup⟩
  · have hi : out.issued = some (receiptId s.gen (s.next + 1)) := by
      simp [Output.issued, hrc, receiptId_ne_empty]
    rw [hi, hres]
    simp only [Option.toList_some, Option.toList_none, List.append_nil]
    have fresh : receiptId s.gen (s.next + 1) ∉ issued := by
      rw [inv.issued_eq]; simp only [List.mem_map, List.mem_range, not_exists, not_and]
      intro k hk he; have := receiptId_inj he; omega
    refine ⟨?_, ?_, ?_, inv.resolved_nodup⟩
    · rw [hn, hg, inv.issued_eq, List.range_succ, List.map_append]; rfl
    · intro r hsome
      rw [hr] at hsome; simp only [update] at hsome
      split at hsome
      · next he => subst he; simp
      · exact List.mem_append_left _ (inv.pending_issued r hsome)
    · intro r hmem
      obtain ⟨hnone, hiss⟩ := inv.resolved_done r hmem
      refine ⟨?_, List.mem_append_left _ hiss⟩
      rw [hr]; simp only [update]
      split
      · next he => subst he; exact absurd hiss fresh
      · exact hnone
  · have hi : out.issued = none := by simp [Output.issued, hrc]
    rw [hi, hres]
    simp only [Option.toList_some, Option.toList_none, List.append_nil]
    have hnotres : id ∉ resolved := fun hm => by
      have := (inv.resolved_done id hm).1; simp [this] at hid
    refine ⟨by rw [hn, hg]; exact inv.issued_eq, ?_, ?_, ?_⟩
    · intro r hsome
      rw [hr] at hsome; simp only [update] at hsome
      split at hsome
      · simp at hsome
      · exact inv.pending_issued r hsome
    · intro r hmem
      rw [hr]; simp only [update]
      rcases List.mem_append.mp hmem with hm | hm
      · obtain ⟨hnone, hiss⟩ := inv.resolved_done r hm
        split
        · exact ⟨rfl, hiss⟩
        · exact ⟨hnone, hiss⟩
      · simp at hm; subst hm; simp [inv.pending_issued r hid]
    · exact List.nodup_append.mpr ⟨inv.resolved_nodup, by simp,
        fun a ha b hb => by simp at hb; subst hb; exact fun he => hnotres (he ▸ ha)⟩

theorem receiptInv_run {s : State} {issued resolved : List String} (inv : ReceiptInv s issued resolved)
    (ops : List Op) :
    ReceiptInv (s.run ops).2 (issued ++ issuedIds (s.run ops).1) (resolved ++ resolvedIds (s.run ops).1) := by
  induction ops generalizing s issued resolved with
  | nil => simpa [State.run, issuedIds, resolvedIds] using inv
  | cons op ops ih =>
    simp only [State.run]
    split
    · simpa [issuedIds, resolvedIds] using ih inv
    · next out s1 h =>
      have := ih (receiptInv_apply inv h)
      rw [issuedIds_ok, resolvedIds_ok]
      simpa only [List.append_assoc] using this

/-- SND-5, SUB-15: over any sequence of calls on a new session, every receipt id
is issued at most once, resolved (by RECEIPT or a matching ERROR) at most
once, and only after it was issued. -/
theorem receipts_issued_once_resolved_at_most_once (g : Nat) (v : Version) (ops : List Op) :
    (issuedIds ((new g v).run ops).1).Nodup ∧ (resolvedIds ((new g v).run ops).1).Nodup ∧
      ∀ r ∈ resolvedIds ((new g v).run ops).1, r ∈ issuedIds ((new g v).run ops).1 := by
  have inv := receiptInv_run (receiptInv_new g v) ops
  simp only [List.nil_append] at inv
  refine ⟨?_, inv.resolved_nodup, fun r hr => (inv.resolved_done r hr).2⟩
  rw [inv.issued_eq]
  exact List.Pairwise.map _ (fun a b hab he => hab (by have := receiptId_inj he; omega)) List.nodup_range

end Stomp.Session
