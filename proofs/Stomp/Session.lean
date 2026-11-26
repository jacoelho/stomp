import Stomp.Frame

/-!
The session core, `session.Session` (internal/session/session.go). Every Go
method leaves the session unchanged when it returns an error, so an operation
here yields either an error or its output with the next state.
-/

namespace Stomp.Session

abbrev Headers := List (String × String)

inductive Mode where
  | client | clientIndividual
  deriving DecidableEq, Repr

def Mode.name : Mode → String
  | .client => "client"
  | .clientIndividual => "client-individual"

inductive Kind where
  | send | subscribe | unsubscribe | ack | disconnect | begin | commit | abort
  deriving DecidableEq, Repr

structure Pending where
  kind : Kind
  sub : String := ""
  tx : String := ""
  deriving DecidableEq, Repr

structure Sub where
  draining : Bool
  /-- Distinguishes subscriptions that reuse an id within a session (SUB-31). -/
  seq : Nat
  deriving DecidableEq, Repr

inductive Error where
  | disconnecting | duplicateID | notSubscribed | staleAck | txAborted | txDone
  | invalidRequest | reservedHeader | textHasNUL | protocol
  deriving DecidableEq, Repr

structure State where
  gen : Nat
  ver : Version
  subs : String → Option Sub
  /-- Open transactions: id to the sequence number of the `Begin` that opened it. -/
  txs : String → Option Nat
  receipts : String → Option Pending
  next : Nat
  nextTx : Nat
  nextSub : Nat
  disconnecting : Bool

def update {α : Type} (m : String → α) (k : String) (a : α) : String → α :=
  fun k' => if k' = k then a else m k'

/-- `session.New`. -/
def new (gen : Nat) (ver : Version) : State :=
  ⟨gen, ver, fun _ => none, fun _ => none, fun _ => none, 0, 0, 0, false⟩

/-- The receipt id `<gen>-<n>`. -/
def receiptId (gen n : Nat) : String := Nat.repr gen ++ "-" ++ Nat.repr n

def State.issue (s : State) (p : Pending) : String × State :=
  let id := receiptId s.gen (s.next + 1)
  (id, { s with next := s.next + 1, receipts := update s.receipts id (some p) })

structure OutFrame where
  command : Command
  headers : Headers
  body : Bytes := []
  deriving DecidableEq, Repr

structure Outgoing where
  destination : String
  contentType : String
  headers : Headers
  body : Bytes
  text : Bool
  deriving DecidableEq, Repr

structure AckHandle where
  gen : Nat
  sub : String
  seq : Nat
  id : String
  deriving DecidableEq, Repr

structure TxHandle where
  gen : Nat
  id : String
  seq : Nat
  deriving DecidableEq, Repr

/-- A frame from the broker, as `Receive` reads it. -/
structure Inbound where
  command : Command
  headers : Headers
  deriving DecidableEq, Repr

inductive Event where
  | deliver (sub : String) (seq : Nat) (ackId : String)
  | discard (sub : String)
  | receipt (id : String) (p : Pending)
  /-- `p` is the pending frame the ERROR's `receipt-id` matched, if any. -/
  | error (id : String) (p : Option Pending)
  deriving DecidableEq, Repr

def checkReserved (reserved : List String) : Headers → Except Error Unit
  | [] => .ok ()
  | (n, _) :: hs =>
    if n = "" then .error .invalidRequest
    else if n ∈ reserved then .error .reservedHeader
    else checkReserved reserved hs

def reservedSend : List String := ["destination", "content-length", "content-type", "receipt", "transaction"]

/-- SUB-6: names the client writes on SUBSCRIBE. -/
def reservedSubscribe : List String := ["id", "destination", "ack", "receipt", "content-length"]

def sendHeaders (m : Outgoing) : Except Error Headers :=
  if m.destination = "" then .error .invalidRequest
  else
    match checkReserved reservedSend m.headers with
    | .error e => .error e
    | .ok () =>
      if m.text ∧ nul ∈ m.body then .error .textHasNUL
      else .ok ([("destination", m.destination)]
        ++ (if m.contentType ≠ "" then [("content-type", m.contentType)] else [])
        ++ (if m.text then [] else [("content-length", Nat.repr m.body.length)]))

def checkTx (s : State) (tx : TxHandle) : Except Error Unit :=
  if s.disconnecting then .error .disconnecting
  else if tx.gen ≠ s.gen then .error .txAborted
  else if s.txs tx.id ≠ some tx.seq then .error .txDone
  else .ok ()

def ackHeaders (s : State) (h : AckHandle) : Except Error Headers :=
  if s.disconnecting then .error .disconnecting
  else if h.gen ≠ s.gen then .error .staleAck
  else
    match s.subs h.sub with
    | none => .error .staleAck
    | some st =>
      if st.draining ∨ st.seq ≠ h.seq then .error .staleAck
      else match s.ver with
        | .v11 => .ok [("message-id", h.id), ("subscription", h.sub)]
        | .v12 => .ok [("id", h.id)]

def ackCommand (nack : Bool) : Command := if nack then .nack else .ack

def State.send (s : State) (m : Outgoing) : Except Error ((OutFrame × String) × State) :=
  if s.disconnecting then .error .disconnecting
  else
    match sendHeaders m with
    | .error e => .error e
    | .ok hs =>
      let (r, s') := s.issue ⟨.send, "", ""⟩
      .ok ((⟨.send, hs ++ [("receipt", r)] ++ m.headers, m.body⟩, r), s')

def State.sendTx (s : State) (tx : TxHandle) (m : Outgoing) : Except Error OutFrame :=
  match checkTx s tx with
  | .error e => .error e
  | .ok () =>
    match sendHeaders m with
    | .error e => .error e
    | .ok hs => .ok ⟨.send, hs ++ [("transaction", tx.id)] ++ m.headers, m.body⟩

def State.subscribe (s : State) (id dest : String) (mode : Mode) (extra : Headers) :
    Except Error ((OutFrame × String) × State) :=
  if s.disconnecting then .error .disconnecting
  else if (s.subs id).isSome then .error .duplicateID
  else match checkReserved reservedSubscribe extra with
  | .error e => .error e
  | .ok () =>
    let s1 := { s with nextSub := s.nextSub + 1, subs := update s.subs id (some ⟨false, s.nextSub + 1⟩) }
    let (r, s2) := s1.issue ⟨.subscribe, id, ""⟩
    .ok ((⟨.subscribe, [("id", id), ("destination", dest), ("ack", mode.name), ("receipt", r)] ++ extra, []⟩, r), s2)

def State.unsubscribe (s : State) (id : String) : Except Error ((OutFrame × String) × State) :=
  if s.disconnecting then .error .disconnecting
  else
    match s.subs id with
    | none => .error .notSubscribed
    | some st =>
      if st.draining then .error .notSubscribed
      else
        let s1 := { s with subs := update s.subs id (some { st with draining := true }) }
        let (r, s2) := s1.issue ⟨.unsubscribe, id, ""⟩
        .ok ((⟨.unsubscribe, [("id", id), ("receipt", r)], []⟩, r), s2)

def State.ack (s : State) (h : AckHandle) (nack wait : Bool) : Except Error ((OutFrame × String) × State) :=
  match ackHeaders s h with
  | .error e => .error e
  | .ok hs =>
    if wait then
      let (r, s') := s.issue ⟨.ack, h.sub, ""⟩
      .ok ((⟨ackCommand nack, hs ++ [("receipt", r)], []⟩, r), s')
    else .ok ((⟨ackCommand nack, hs, []⟩, ""), s)

def State.ackTx (s : State) (tx : TxHandle) (h : AckHandle) (nack : Bool) : Except Error OutFrame :=
  match checkTx s tx with
  | .error e => .error e
  | .ok () =>
    match ackHeaders s h with
    | .error e => .error e
    | .ok hs => .ok ⟨ackCommand nack, hs ++ [("transaction", tx.id)], []⟩

def State.begin (s : State) (id : String) : Except Error ((OutFrame × TxHandle × String) × State) :=
  if s.disconnecting then .error .disconnecting
  else if (s.txs id).isSome then .error .invalidRequest
  else
    let s1 := { s with nextTx := s.nextTx + 1, txs := update s.txs id (some (s.nextTx + 1)) }
    let (r, s2) := s1.issue ⟨.begin, "", id⟩
    .ok ((⟨.begin, [("transaction", id), ("receipt", r)], []⟩, ⟨s.gen, id, s.nextTx + 1⟩, r), s2)

def State.finish (s : State) (tx : TxHandle) (abort : Bool) : Except Error ((OutFrame × String) × State) :=
  match checkTx s tx with
  | .error e => .error e
  | .ok () =>
    let s1 := { s with txs := update s.txs tx.id none }
    let (r, s2) := s1.issue ⟨if abort then .abort else .commit, "", tx.id⟩
    .ok ((⟨if abort then .abort else .commit, [("transaction", tx.id), ("receipt", r)], []⟩, r), s2)

/-- `Disconnect`: a session builds at most one DISCONNECT. -/
def State.disconnect (s : State) : Except Error ((OutFrame × String) × State) :=
  if s.disconnecting then .error .disconnecting
  else
    let (r, s') := { s with disconnecting := true }.issue ⟨.disconnect, "", ""⟩
    .ok ((⟨.disconnect, [("receipt", r)], []⟩, r), s')

/-- `End`: no frame; the subscription's MESSAGE frames are discarded from then on. -/
def State.endSub (s : State) (id : String) : State :=
  match s.subs id with
  | none => s
  | some st => { s with subs := update s.subs id (some { st with draining := true }) }

/-- `stomptext.Frame.Header`: the first value named `name`. -/
def Inbound.header (f : Inbound) (name : String) : Option String :=
  (f.headers.find? (·.1 = name)).map (·.2)

/-- The MESSAGE header carrying the acknowledgement id (SUB-23, SUB-27). -/
def ackIdHeader : Version → String
  | .v11 => "message-id"
  | .v12 => "ack"

def State.receive (s : State) (f : Inbound) : Except Error (Event × State) :=
  match f.command with
  | .message =>
    match f.header "subscription" with
    | none => .error .protocol
    | some id =>
      match s.subs id with
      | none => .ok (.discard id, s)
      | some st =>
        if st.draining then .ok (.discard id, s)
        else
          match f.header (ackIdHeader s.ver) with
          | none => .error .protocol
          | some a => .ok (.deliver id st.seq a, s)
  | .receipt =>
    let id := (f.header "receipt-id").getD ""
    match s.receipts id with
    | none => .error .protocol
    | some p =>
      .ok (.receipt id p,
        { s with receipts := update s.receipts id none,
                 subs := if p.kind = .unsubscribe then update s.subs p.sub none else s.subs })
  | .error =>
    let id := (f.header "receipt-id").getD ""
    match s.receipts id with
    | none => .ok (.error id none, s)
    | some p => .ok (.error id (some p), { s with receipts := update s.receipts id none })
  | _ => .error .protocol

/-- One call on a session. -/
inductive Op where
  | subscribe (id dest : String) (mode : Mode) (extra : Headers)
  | unsubscribe (id : String)
  | ack (h : AckHandle) (nack wait : Bool)
  | ackTx (tx : TxHandle) (h : AckHandle) (nack : Bool)
  | send (m : Outgoing)
  | sendTx (tx : TxHandle) (m : Outgoing)
  | begin (id : String)
  | finish (tx : TxHandle) (abort : Bool)
  | disconnect
  | endSub (id : String)
  | receive (f : Inbound)
  deriving DecidableEq, Repr

/-- What a successful call returned. `receipt` is `""` when it requested none. -/
structure Output where
  frame : Option OutFrame := none
  receipt : String := ""
  tx : Option TxHandle := none
  event : Option Event := none
  deriving DecidableEq, Repr

def State.apply (s : State) : Op → Except Error (Output × State)
  | .subscribe id dest mode extra =>
    (s.subscribe id dest mode extra).map fun ((f, r), s') => ({ frame := f, receipt := r }, s')
  | .unsubscribe id => (s.unsubscribe id).map fun ((f, r), s') => ({ frame := f, receipt := r }, s')
  | .ack h nack wait => (s.ack h nack wait).map fun ((f, r), s') => ({ frame := f, receipt := r }, s')
  | .ackTx tx h nack => (s.ackTx tx h nack).map fun f => ({ frame := f }, s)
  | .send m => (s.send m).map fun ((f, r), s') => ({ frame := f, receipt := r }, s')
  | .sendTx tx m => (s.sendTx tx m).map fun f => ({ frame := f }, s)
  | .begin id => (s.begin id).map fun ((f, t, r), s') => ({ frame := f, receipt := r, tx := t }, s')
  | .finish tx abort => (s.finish tx abort).map fun ((f, r), s') => ({ frame := f, receipt := r }, s')
  | .disconnect => s.disconnect.map fun ((f, r), s') => ({ frame := f, receipt := r }, s')
  | .endSub id => .ok ({}, s.endSub id)
  | .receive f => (s.receive f).map fun (e, s') => ({ event := e }, s')

/-- Applies `ops` in order; a failed call leaves the state as it was. -/
def State.run (s : State) : List Op → List (Except Error Output) × State
  | [] => ([], s)
  | op :: ops =>
    match s.apply op with
    | .error e => let (outs, s') := s.run ops; (.error e :: outs, s')
    | .ok (out, s1) => let (outs, s') := s1.run ops; (.ok out :: outs, s')

end Stomp.Session
