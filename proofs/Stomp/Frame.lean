/-!
Frame vocabulary of `stomptext` (stomptext/frame.go): versions, commands,
headers, frames and the error kinds of decoding and encoding.
-/

namespace Stomp

abbrev Bytes := List UInt8

def lf : UInt8 := 10
def cr : UInt8 := 13
def colon : UInt8 := 58
def backslash : UInt8 := 92
def nul : UInt8 := 0

inductive Version where
  | v11
  | v12
  deriving DecidableEq, Repr

inductive Command where
  | connect | stomp | connected | send | subscribe | unsubscribe | ack | nack
  | begin | commit | abort | disconnect | message | receipt | error
  deriving DecidableEq, Repr

namespace Command

def all : List Command :=
  [connect, stomp, connected, send, subscribe, unsubscribe, ack, nack,
   begin, commit, abort, disconnect, message, receipt, error]

/-- The ASCII octets of the command line. -/
def name : Command → Bytes
  | connect => [67, 79, 78, 78, 69, 67, 84]
  | stomp => [83, 84, 79, 77, 80]
  | connected => [67, 79, 78, 78, 69, 67, 84, 69, 68]
  | send => [83, 69, 78, 68]
  | subscribe => [83, 85, 66, 83, 67, 82, 73, 66, 69]
  | unsubscribe => [85, 78, 83, 85, 66, 83, 67, 82, 73, 66, 69]
  | ack => [65, 67, 75]
  | nack => [78, 65, 67, 75]
  | begin => [66, 69, 71, 73, 78]
  | commit => [67, 79, 77, 77, 73, 84]
  | abort => [65, 66, 79, 82, 84]
  | disconnect => [68, 73, 83, 67, 79, 78, 78, 69, 67, 84]
  | message => [77, 69, 83, 83, 65, 71, 69]
  | receipt => [82, 69, 67, 69, 73, 80, 84]
  | error => [69, 82, 82, 79, 82]

/-- Headers of CONNECT and CONNECTED are not escaped (FRM-11). -/
def escaped (c : Command) : Bool := c != connect && c != connected

def bodyAllowed (c : Command) : Bool := c == send || c == message || c == error

end Command

def lookupCommand (b : Bytes) : Option Command := Command.all.find? (·.name == b)

structure Header where
  name : Bytes
  value : Bytes
  deriving DecidableEq, Repr

structure Frame where
  command : Command
  headers : List Header
  body : Bytes
  deriving DecidableEq, Repr

/-- `content-length` -/
def contentLengthName : Bytes := [99, 111, 110, 116, 101, 110, 116, 45, 108, 101, 110, 103, 116, 104]

/-- The Go sentinel errors of `stomptext`, plus the two `io` ends of input.
`endOfInput` is `io.EOF`: the input ended between frames. -/
inductive ErrorKind where
  | endOfInput
  | unexpectedEOF
  | unknownCommand
  | invalidEscape
  | invalidHeader
  | invalidContentLength
  | missingNUL
  | bodyNotAllowed
  | invalidEOL
  | nulInBody
  deriving DecidableEq, Repr

/-- Splits at the first occurrence of `b`, dropping it. -/
def splitOn? (b : UInt8) : Bytes → Option (Bytes × Bytes)
  | [] => none
  | c :: r => if c = b then some ([], r) else (splitOn? b r).map fun p => (c :: p.1, p.2)

def isDigit (c : UInt8) : Bool := 48 ≤ c && c ≤ 57

def digitsValue (b : Bytes) : Nat := b.foldl (fun n c => n * 10 + (c.toNat - 48)) 0

/-- `parseLength`: one or more decimal digits. -/
def parseLength (b : Bytes) : Option Nat :=
  if b ≠ [] ∧ b.all isDigit then some (digitsValue b) else none

end Stomp
