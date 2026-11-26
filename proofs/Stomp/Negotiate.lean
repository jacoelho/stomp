import Stomp.Frame

/-! `session.Negotiate` (internal/session/session.go). Durations are `Int`
nanoseconds, as `time.Duration` is. -/

namespace Stomp

def comma : UInt8 := 44

/-- `strconv.ParseUint(s, 10, 31)`: decimal digits only, below 2³¹. -/
def parseUint31 (s : Bytes) : Option Nat :=
  if s ≠ [] ∧ s.all isDigit ∧ digitsValue s < 2 ^ 31 then some (digitsValue s) else none

structure Heartbeat where
  send : Int
  recv : Int
  deriving DecidableEq, Repr

def pick (mine theirs : Int) : Int := if mine = 0 ∨ theirs = 0 then 0 else max mine theirs

/-- The negotiated intervals, or `none` for Go's `ErrProtocol`. `header` is the
CONNECTED `heart-beat` value, `none` when the header is absent. -/
def negotiate (send expect : Int) (header : Option Bytes) : Option Heartbeat :=
  match header with
  | none => some ⟨0, 0⟩
  | some h =>
    match splitOn? comma h with
    | none => none
    | some (sx, sy) =>
      match parseUint31 sx, parseUint31 sy with
      | some x, some y => some ⟨pick send (y * 1000000), pick expect (x * 1000000)⟩
      | _, _ => none

end Stomp
