import Stomp.Frame

/-! Header value escaping: `appendEscaped` and `Decoder.unescape` in stomptext. -/

namespace Stomp

def escapeByte (v : Version) (c : UInt8) : Bytes :=
  if c = backslash then [backslash, backslash]
  else if c = lf then [backslash, 110]
  else if c = colon then [backslash, 99]
  else if c = cr ∧ v = .v12 then [backslash, 114]
  else [c]

def escape (v : Version) (s : Bytes) : Bytes := s.flatMap (escapeByte v)

/-- The octet an escape `\d` stands for; `\r` exists only in 1.2 (FRM-31). -/
def unescapeByte (v : Version) (d : UInt8) : Option UInt8 :=
  if d = 110 then some lf
  else if d = 99 then some colon
  else if d = backslash then some backslash
  else if d = 114 ∧ v = .v12 then some cr
  else none

def unescape (v : Version) : Bytes → Except ErrorKind Bytes
  | [] => .ok []
  | c :: r =>
    if c = backslash then
      match r with
      | [] => .error .invalidEscape
      | d :: r' =>
        match unescapeByte v d with
        | none => .error .invalidEscape
        | some e => (e :: ·) <$> unescape v r'
    else (c :: ·) <$> unescape v r

end Stomp
