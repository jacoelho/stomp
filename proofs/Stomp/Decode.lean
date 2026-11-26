import Stomp.Escape

/-!
Whole-input decoding: `Decoder.ReadFrame` (stomptext/decode.go) applied to an
input that is entirely available. Frame limits are not modelled: the oracle
tests run the Go decoder with every limit at `math.MaxInt`.
-/

namespace Stomp

theorem splitOn?_length {b : UInt8} :
    ∀ {l line rest : Bytes}, splitOn? b l = some (line, rest) → rest.length < l.length
  | [], _, _, h => by simp [splitOn?] at h
  | c :: r, line, rest, h => by
    simp only [splitOn?] at h
    split at h
    · simp at h; simp [← h.2]
    · cases e : splitOn? b r with
      | none => simp [e] at h
      | some p =>
        obtain ⟨l', r'⟩ := p
        simp [e] at h
        have := splitOn?_length e
        simp [← h.2]; omega

/-- `skipEOL`: heart-beat end-of-lines before a frame. In 1.1 a CR is an
ordinary octet (FRM-30); in 1.2 it must start a CRLF. -/
def skipEOL (v : Version) : Bytes → Except ErrorKind Bytes
  | [] => .error .endOfInput
  | c :: r =>
    if c = lf then skipEOL v r
    else if c = cr then
      match v with
      | .v11 => .ok (c :: r)
      | .v12 =>
        match r with
        | [] => .error .unexpectedEOF
        | d :: r' => if d = lf then skipEOL v r' else .error .invalidEOL
    else .ok (c :: r)

/-- The line that ends the header section: empty, or in 1.2 a lone CR. -/
def blankLine (v : Version) (line : Bytes) : Bool :=
  line.isEmpty || (v == .v12 && line == [cr])

/-- The header lines after the command line, up to the blank line, and the
octets after it; `none` when no blank line ends the section. -/
def headerLines (v : Version) (b : Bytes) : Option (List Bytes × Bytes) :=
  match _hsplit : splitOn? lf b with
  | none => none
  | some (line, rest) =>
    if blankLine v line then some ([], rest)
    else (headerLines v rest).map fun p => (line :: p.1, p.2)
termination_by b.length
decreasing_by exact splitOn?_length _hsplit

def trimCR (v : Version) (line : Bytes) : Bytes :=
  if v = .v12 ∧ line.getLast? = some cr then line.dropLast else line

def unescapeIf (esc : Bool) (v : Version) (s : Bytes) : Except ErrorKind Bytes :=
  if esc then unescape v s else .ok s

/-- `parseHead` after the command: each line splits at its first colon, which
must not be the first octet; the first header named `content-length` gives the
body length. -/
def parseHeaders (v : Version) (esc : Bool) :
    List Bytes → Option Nat → Except ErrorKind (List Header × Option Nat)
  | [], cl => .ok ([], cl)
  | line :: lines, cl =>
    match splitOn? colon (trimCR v line) with
    | none => .error .invalidHeader
    | some (rawName, rawValue) =>
      if rawName = [] then .error .invalidHeader
      else
        match unescapeIf esc v rawName, unescapeIf esc v rawValue with
        | .error e, _ => .error e
        | .ok _, .error e => .error e
        | .ok name, .ok value =>
          let cl' : Except ErrorKind (Option Nat) :=
            match cl with
            | some n => .ok (some n)
            | none =>
              if name = contentLengthName then
                match parseLength value with
                | some n => .ok (some n)
                | none => .error .invalidContentLength
              else .ok none
          match cl' with
          | .error e => .error e
          | .ok cl' =>
            match parseHeaders v esc lines cl' with
            | .error e => .error e
            | .ok (hs, clf) => .ok (⟨name, value⟩ :: hs, clf)

/-- The body and the octets after its NUL: FRM-5 with a length, FRM-6 without. -/
def readBody : Option Nat → Bytes → Except ErrorKind (Bytes × Bytes)
  | some n, b =>
    if b.length < n + 1 then .error .unexpectedEOF
    else if b[n]? = some nul then .ok (b.take n, b.drop (n + 1))
    else .error .missingNUL
  | none, b =>
    match splitOn? nul b with
    | none => .error .unexpectedEOF
    | some p => .ok p

/-- `Decoder.ReadFrame` on a whole input: the frame and the octets after it. -/
def decodeFrame (v : Version) (input : Bytes) : Except ErrorKind (Frame × Bytes) :=
  match skipEOL v input with
  | .error e => .error e
  | .ok b =>
    match splitOn? lf b with
    | none => .error .unexpectedEOF
    | some (cmdLine, afterCmd) =>
      match headerLines v afterCmd with
      | none => .error .unexpectedEOF
      | some (lines, afterHead) =>
        match lookupCommand (trimCR v cmdLine) with
        | none => .error .unknownCommand
        | some cmd =>
          match parseHeaders v cmd.escaped lines none with
          | .error e => .error e
          | .ok (hs, cl) =>
            match readBody cl afterHead with
            | .error e => .error e
            | .ok (body, rest) =>
              if body ≠ [] ∧ cmd.bodyAllowed = false then .error .bodyNotAllowed
              else .ok (⟨cmd, hs, body⟩, rest)

/-- Repeated `ReadFrame` until its first error: the frames read, the input
after the last of them, and the error (`endOfInput` for a clean end). Each frame
consumes at least its NUL, so `input.length + 1` steps suffice. -/
def decodeAll (v : Version) (input : Bytes) : List Frame × Bytes × ErrorKind :=
  go (input.length + 1) input
where
  go : Nat → Bytes → List Frame × Bytes × ErrorKind
    | 0, b => ([], b, .endOfInput)
    | fuel + 1, b =>
      match decodeFrame v b with
      | .error e => ([], b, e)
      | .ok (f, rest) =>
        let (fs, r, e) := go fuel rest
        (f :: fs, r, e)

end Stomp
