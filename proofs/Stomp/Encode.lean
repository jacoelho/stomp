import Stomp.Escape

/-! `AppendFrame` (stomptext/encode.go). -/

namespace Stomp

/-- `appendRaw`: an unescaped name or value must not break its line, nor, for a
name, the colon separator. -/
def rawOK (v : Version) (isName : Bool) (s : Bytes) : Bool :=
  s.all fun c => c != lf && !(c == cr && v == .v12) && !(isName && c == colon)

/-- The encoder's content-length rule: the body length, with no leading zero. -/
def clValid (value : Bytes) (bodyLen : Nat) : Bool :=
  match parseLength value with
  | some n => n == bodyLen && !(value.length > 1 && value.head? == some 48)
  | none => false

def headerLine (v : Version) (esc : Bool) (h : Header) : Option Bytes :=
  if esc then some (escape v h.name ++ colon :: escape v h.value)
  else if rawOK v true h.name && rawOK v false h.value then some (h.name ++ colon :: h.value)
  else none

/-- The header section and whether a `content-length` header was seen. -/
def encodeHeaders (v : Version) (esc : Bool) (bodyLen : Nat) :
    Bool → List Header → Except ErrorKind (Bytes × Bool)
  | seen, [] => .ok ([], seen)
  | seen, h :: hs =>
    if h.name = [] then .error .invalidHeader
    else if h.name = contentLengthName ∧ seen = false ∧ clValid h.value bodyLen = false then
      .error .invalidContentLength
    else
      match headerLine v esc h with
      | none => .error .invalidHeader
      | some line =>
        match encodeHeaders v esc bodyLen (seen || decide (h.name = contentLengthName)) hs with
        | .error e => .error e
        | .ok (rest, seen') => .ok (line ++ lf :: rest, seen')

def encode (v : Version) (f : Frame) : Except ErrorKind Bytes :=
  if f.body ≠ [] ∧ f.command.bodyAllowed = false then .error .bodyNotAllowed
  else
    match encodeHeaders v f.command.escaped f.body.length false f.headers with
    | .error e => .error e
    | .ok (hb, seen) =>
      if seen = false ∧ nul ∈ f.body then .error .nulInBody
      else .ok (f.command.name ++ lf :: hb ++ lf :: f.body ++ [nul])

end Stomp
