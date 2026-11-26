import Lean.Data.Json
import Stomp

/-!
`stomp-oracle`: reads one JSON request per line on stdin and writes one JSON
response per line on stdout. The protocol is documented in proofs/README.md.
-/

open Lean Stomp Stomp.Session

namespace Oracle

def hexDigit (n : Nat) : Char := Nat.digitChar n

def toHex (b : Bytes) : String :=
  String.ofList (b.flatMap fun c => [hexDigit (c.toNat / 16), hexDigit (c.toNat % 16)])

def hexValue (c : Char) : Except String Nat :=
  if '0' ≤ c ∧ c ≤ '9' then .ok (c.toNat - '0'.toNat)
  else if 'a' ≤ c ∧ c ≤ 'f' then .ok (c.toNat - 'a'.toNat + 10)
  else if 'A' ≤ c ∧ c ≤ 'F' then .ok (c.toNat - 'A'.toNat + 10)
  else .error s!"bad hex digit {c}"

def ofHexList : List Char → Except String Bytes
  | [] => .ok []
  | [_] => .error "odd hex length"
  | a :: b :: r => do
    let hi ← hexValue a
    let lo ← hexValue b
    let rest ← ofHexList r
    pure ((hi * 16 + lo).toUInt8 :: rest)

def ofHex (s : String) : Except String Bytes := ofHexList s.toList

def bytesOf (s : String) : Bytes := s.toUTF8.toList

def stringOf (b : Bytes) : String := String.fromUTF8! ⟨b.toArray⟩

def parseVersion : String → Except String Version
  | "1.1" => .ok .v11
  | "1.2" => .ok .v12
  | v => .error s!"unknown version {v}"

def errorName : ErrorKind → String
  | .endOfInput => "eof"
  | .unexpectedEOF => "unexpected-eof"
  | .unknownCommand => "ErrUnknownCommand"
  | .invalidEscape => "ErrInvalidEscape"
  | .invalidHeader => "ErrInvalidHeader"
  | .invalidContentLength => "ErrInvalidContentLength"
  | .missingNUL => "ErrMissingNUL"
  | .bodyNotAllowed => "ErrBodyNotAllowed"
  | .invalidEOL => "ErrInvalidEOL"
  | .nulInBody => "ErrNULInBody"

def sessionErrorName : Session.Error → String
  | .disconnecting => "ErrDisconnecting"
  | .duplicateID => "ErrDuplicateID"
  | .notSubscribed => "ErrNotSubscribed"
  | .staleAck => "ErrStaleAck"
  | .txAborted => "ErrTxAborted"
  | .txDone => "ErrTxDone"
  | .invalidRequest => "ErrInvalidRequest"
  | .reservedHeader => "ErrReservedHeader"
  | .textHasNUL => "ErrTextHasNUL"
  | .protocol => "ErrProtocol"

def commandName (c : Stomp.Command) : String := stringOf c.name

def parseCommand (s : String) : Except String Stomp.Command :=
  match lookupCommand (bytesOf s) with
  | some c => .ok c
  | none => .error s!"unknown command {s}"

def frameJson (f : Frame) : Json :=
  Json.mkObj [
    ("command", commandName f.command),
    ("headers", Json.arr (f.headers.map fun h => Json.arr #[toHex h.name, toHex h.value]).toArray),
    ("body", toHex f.body)]

def field (j : Json) (k : String) : Except String Json := j.getObjVal? k

def str (j : Json) (k : String) : Except String String := j.getObjValAs? String k

def bool (j : Json) (k : String) : Except String Bool := j.getObjValAs? Bool k

def nat (j : Json) (k : String) : Except String Nat := j.getObjValAs? Nat k

def int (j : Json) (k : String) : Except String Int := j.getObjValAs? Int k

def pairs (j : Json) (k : String) : Except String (List (String × String)) := do
  let a ← j.getObjValAs? (Array (Array String)) k
  a.toList.mapM fun p =>
    match p.toList with
    | [n, v] => pure (n, v)
    | _ => throw s!"{k}: expected [name, value]"

def decodeReq (j : Json) : Except String Json := do
  let v ← parseVersion (← str j "version")
  let input ← ofHex (← str j "input")
  let (frames, rest, err) := decodeAll v input
  pure <| Json.mkObj [
    ("frames", Json.arr (frames.map frameJson).toArray),
    ("rest", toHex rest),
    ("error", errorName err)]

def encodeReq (j : Json) : Except String Json := do
  let v ← parseVersion (← str j "version")
  let hs ← (← pairs j "headers").mapM fun (n, val) => do pure (Header.mk (← ofHex n) (← ofHex val))
  let body ← ofHex (← str j "body")
  match lookupCommand (bytesOf (← str j "command")) with
  | none => pure (Json.mkObj [("error", errorName .unknownCommand)])
  | some cmd =>
    match encode v ⟨cmd, hs, body⟩ with
    | .ok out => pure (Json.mkObj [("output", toHex out)])
    | .error e => pure (Json.mkObj [("error", errorName e)])

def negotiateReq (j : Json) : Except String Json := do
  let header ← match j.getObjValD "header" with
    | Json.null => pure none
    | h => do pure (some (← ofHex (← h.getStr?)))
  match negotiate (← int j "send") (← int j "expect") header with
  | some hb => pure (Json.mkObj [("send", hb.send), ("recv", hb.recv)])
  | none => pure (Json.mkObj [("error", "ErrProtocol")])

def parseMode : String → Except String Mode
  | "client" => .ok .client
  | "client-individual" => .ok .clientIndividual
  | m => .error s!"unknown mode {m}"

def kindName : Kind → String
  | .send => "send" | .subscribe => "subscribe"
  | .unsubscribe => "unsubscribe" | .ack => "ack" | .disconnect => "disconnect"
  | .begin => "begin" | .commit => "commit" | .abort => "abort"

def txOf (j : Json) : Except String TxHandle := do
  let t ← field j "tx"
  pure ⟨← nat t "gen", ← str t "id", ← nat t "seq"⟩

def outgoingOf (j : Json) : Except String Outgoing := do
  let m ← field j "msg"
  pure ⟨← str m "destination", ← str m "contentType", ← pairs m "headers", ← ofHex (← str m "body"), ← bool m "text"⟩

def opOf (j : Json) : Except String Op := do
  match ← str j "do" with
  | "subscribe" => pure (.subscribe (← str j "id") (← str j "dest") (← parseMode (← str j "mode")) (← pairs j "extra"))
  | "unsubscribe" => pure (.unsubscribe (← str j "id"))
  | "ack" => pure (.ack ⟨← nat j "gen", ← str j "sub", ← nat j "seq", ← str j "ackId"⟩ (← bool j "nack") (← bool j "wait"))
  | "ackTx" => pure (.ackTx (← txOf j) ⟨← nat j "gen", ← str j "sub", ← nat j "seq", ← str j "ackId"⟩ (← bool j "nack"))
  | "send" => pure (.send (← outgoingOf j))
  | "sendTx" => pure (.sendTx (← txOf j) (← outgoingOf j))
  | "begin" => pure (.begin (← str j "id"))
  | "finish" => pure (.finish (← txOf j) (← bool j "abort"))
  | "disconnect" => pure .disconnect
  | "end" => pure (.endSub (← str j "id"))
  | "receive" => pure (.receive ⟨← parseCommand (← str j "command"), ← pairs j "headers"⟩)
  | d => throw s!"unknown step {d}"

def pairsJson (hs : List (String × String)) : Json :=
  Json.arr (hs.map fun (n, v) => Json.arr #[n, v]).toArray

def pendingJson (p : Pending) : Json :=
  Json.mkObj [("kind", kindName p.kind), ("sub", p.sub), ("tx", p.tx)]

def eventJson : Event → Json
  | .deliver sub seq ackId => Json.mkObj [("kind", "deliver"), ("sub", sub), ("seq", seq), ("ackId", ackId)]
  | .discard sub => Json.mkObj [("kind", "discard"), ("sub", sub)]
  | .receipt id p => Json.mkObj [("kind", "receipt"), ("receipt", id), ("pending", pendingJson p)]
  | .error id p => Json.mkObj [("kind", "error"), ("receipt", id), ("known", p.isSome),
      ("pending", match p with | some p => pendingJson p | none => Json.null)]

def outputJson : Except Session.Error Output → Json
  | .error e => Json.mkObj [("error", sessionErrorName e)]
  | .ok o => Json.mkObj [
      ("frame", match o.frame with
        | some f => Json.mkObj [("command", commandName f.command), ("headers", pairsJson f.headers), ("body", toHex f.body)]
        | none => Json.null),
      ("receipt", o.receipt),
      ("tx", match o.tx with
        | some t => Json.mkObj [("gen", t.gen), ("id", t.id), ("seq", t.seq)]
        | none => Json.null),
      ("event", match o.event with
        | some e => eventJson e
        | none => Json.null)]

def sessionReq (j : Json) : Except String Json := do
  let v ← parseVersion (← str j "version")
  let ops ← (← j.getObjValAs? (Array Json) "trace").toList.mapM opOf
  let (outs, _) := (new (← nat j "gen") v).run ops
  pure (Json.mkObj [("steps", Json.arr (outs.map outputJson).toArray)])

def handle (line : String) : Json :=
  let r : Except String Json := do
    let j ← Json.parse line
    match ← str j "op" with
    | "decode" => decodeReq j
    | "encode" => encodeReq j
    | "negotiate" => negotiateReq j
    | "session" => sessionReq j
    | op => throw s!"unknown op {op}"
  match r with
  | .ok j => j
  | .error e => Json.mkObj [("badRequest", e)]

end Oracle

partial def loop (stdin : IO.FS.Stream) (stdout : IO.FS.Stream) : IO Unit := do
  let line ← stdin.getLine
  if line.isEmpty then return
  let line := line.trimAscii.toString
  unless line.isEmpty do
    stdout.putStrLn (Oracle.handle line).compress
    stdout.flush
  loop stdin stdout

def main : IO Unit := do
  loop (← IO.getStdin) (← IO.getStdout)
