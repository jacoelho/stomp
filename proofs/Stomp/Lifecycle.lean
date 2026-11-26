/-!
The Client's lifecycle as client.go keeps it under `c.mu`: `c.life`, the
`State` it reports, whether a session runs (`c.sess ≠ nil`), and whether a
handshake has completed that supervise has not yet passed to `beginSession`.
Each event is one critical section; `step` returns `none` when the code
cannot reach that section in the given state.
-/

namespace Stomp.Lifecycle

/-- `lifecycle` in client.go. -/
inductive Life where
  | running | closing | closed
  deriving DecidableEq, Repr

/-- `stomp.State`, the state OnState and `Client.State` report. -/
inductive Reported where
  | connecting | connected | waiting | closed
  deriving DecidableEq, Repr

structure State where
  life : Life
  reported : Reported
  session : Bool
  handshook : Bool
  deriving DecidableEq, Repr

/-- The Client as `Open` returns it: running a session. -/
def init : State := ⟨.running, .connected, true, false⟩

inductive Event where
  /-- `Client.Close`: running becomes closing; a later Close changes nothing. -/
  | close
  /-- The locked cleanup at the end of `runSession` (RCN-15). -/
  | sessionEnd
  /-- `backoff`, before the next connection attempt. -/
  | backoff
  /-- `establish` starting an attempt. -/
  | attempt
  /-- `connect` completing a dial and handshake. -/
  | handshake
  /-- `beginSession`, which closes the new connection once Close began (CON-30). -/
  | begin
  /-- `finish`, after Close or when reconnecting gives up. -/
  | finish
  deriving DecidableEq, Repr

def step (s : State) : Event → Option State
  | .close => some (if s.life = .running then { s with life := .closing } else s)
  | .sessionEnd =>
    if s.session then
      some { s with session := false,
                    reported := if s.life = .running then .waiting else s.reported }
    else none
  | .backoff =>
    if s.life = .running ∧ !s.session ∧ !s.handshook then some { s with reported := .waiting } else none
  | .attempt =>
    if s.life = .running ∧ !s.session ∧ !s.handshook then some { s with reported := .connecting } else none
  | .handshake =>
    if s.life = .running ∧ !s.session ∧ !s.handshook then some { s with handshook := true } else none
  | .begin =>
    if s.handshook then
      if s.life = .running then some { s with handshook := false, session := true, reported := .connected }
      else some { s with handshook := false }
    else none
  | .finish =>
    if s.life ≠ .closed ∧ !s.session ∧ !s.handshook then
      some { s with life := .closed, reported := .closed }
    else none

/-- The states the Client reaches from `init`, by any sequence of events. -/
inductive Reachable : State → Prop where
  | init : Reachable init
  | step {s s' : State} (e : Event) : Reachable s → step s e = some s' → Reachable s'

/-- The order the lifecycle moves in. -/
def Life.rank : Life → Nat
  | .running => 0
  | .closing => 1
  | .closed => 2

end Stomp.Lifecycle
