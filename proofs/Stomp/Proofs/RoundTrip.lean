import Stomp.Decode
import Stomp.Encode
import Stomp.Proofs.Bytes
import Stomp.Proofs.Escape

namespace Stomp

/-! ## Heart-beats (FRM-3, FRM-30) -/

/-- An end-of-line the decoder skips before a frame: LF, and in 1.2 also CRLF. -/
def IsEOL (v : Version) (e : Bytes) : Prop := e = [lf] ∨ (v = .v12 ∧ e = [cr, lf])

theorem skipEOL_eol {v : Version} {e : Bytes} (he : IsEOL v e) (input : Bytes) :
    skipEOL v (e ++ input) = skipEOL v input := by
  rcases he with rfl | ⟨rfl, rfl⟩
  · rw [skipEOL.eq_def]; simp
  · rw [skipEOL.eq_def]; simp [cr, lf]

/-- FRM-3, FRM-30: end-of-lines before a frame (LF, and in 1.2 CRLF) do not
change what is decoded, nor the octets left after it. -/
theorem decodeFrame_heartbeats {v : Version} (eols : List Bytes) (h : ∀ e ∈ eols, IsEOL v e)
    (input : Bytes) : decodeFrame v (eols.flatten ++ input) = decodeFrame v input := by
  have : skipEOL v (eols.flatten ++ input) = skipEOL v input := by
    induction eols with
    | nil => rfl
    | cons e es ih =>
      rw [List.flatten_cons, List.append_assoc, skipEOL_eol (h e (List.mem_cons_self ..)),
        ih fun e he => h e (List.mem_cons_of_mem _ he)]
  unfold decodeFrame; rw [this]

/-- FRM-30: in 1.1 a CR before a frame is not an end-of-line; it starts the
command line. -/
theorem skipEOL_v11_cr (r : Bytes) : skipEOL .v11 (cr :: r) = .ok (cr :: r) := by
  rw [skipEOL.eq_def]; simp [cr, lf]

/-! ## Valid frames -/

def firstContentLength : List Header → Option Bytes
  | [] => none
  | h :: hs => if h.name = contentLengthName then some h.value else firstContentLength hs

/-- The frames `AppendFrame` encodes; `encode_ok_iff` shows it is exactly the
encoder's success condition. The command is known by construction. -/
structure ValidFrame (v : Version) (f : Frame) : Prop where
  /-- Only SEND, MESSAGE and ERROR carry a body. -/
  body_allowed : f.body ≠ [] → f.command.bodyAllowed = true
  names_nonempty : ∀ h ∈ f.headers, h.name ≠ []
  /-- CONNECT and CONNECTED headers are written raw, so they cannot hold LF, a
  colon in the name, or in 1.2 a CR. -/
  representable : f.command.escaped = false →
    ∀ h ∈ f.headers, rawOK v true h.name = true ∧ rawOK v false h.value = true
  /-- The first content-length gives the body length, without leading zeros;
  without one the body ends at its first NUL, so it must have none. -/
  content_length :
    match firstContentLength f.headers with
    | some value => clValid value f.body.length = true
    | none => nul ∉ f.body

theorem headerLine_isSome {v : Version} {esc : Bool} {h : Header} :
    (headerLine v esc h).isSome = true ↔
      (esc = false → rawOK v true h.name = true ∧ rawOK v false h.value = true) := by
  unfold headerLine
  cases esc <;> simp

theorem encodeHeaders_seen {v : Version} {esc : Bool} {n : Nat} :
    ∀ {hs : List Header} {seen : Bool} {hb : Bytes} {seen' : Bool},
      encodeHeaders v esc n seen hs = .ok (hb, seen') →
        seen' = (seen || (firstContentLength hs).isSome)
  | [], seen, hb, seen', h => by simp [encodeHeaders] at h; simp [h, firstContentLength]
  | hd :: hs, seen, hb, seen', h => by
    simp only [encodeHeaders] at h
    split at h; · simp at h
    split at h; · simp at h
    split at h; · simp at h
    split at h; · simp at h
    next hr =>
      simp at h; obtain ⟨-, rfl⟩ := h
      have := encodeHeaders_seen hr
      rw [this, firstContentLength]
      by_cases hc : hd.name = contentLengthName <;> simp [hc]

theorem encodeHeaders_ok_iff {v : Version} {esc : Bool} {n : Nat} :
    ∀ {hs : List Header} {seen : Bool},
      (∃ r, encodeHeaders v esc n seen hs = .ok r) ↔
        (∀ h ∈ hs, h.name ≠ [] ∧ (headerLine v esc h).isSome = true) ∧
          (seen = false → ∀ val, firstContentLength hs = some val → clValid val n = true)
  | [], seen => by simp [encodeHeaders, firstContentLength]
  | hd :: hs, seen => by
    have ih := @encodeHeaders_ok_iff v esc n hs (seen || decide (hd.name = contentLengthName))
    simp only [encodeHeaders]
    constructor
    · rintro ⟨r, h⟩
      split at h; · simp at h
      next hn =>
      split at h; · simp at h
      next hcl =>
      split at h; · simp at h
      next line hl =>
      split at h; · simp at h
      next rest seen' hr =>
      obtain ⟨h1, h2⟩ := ih.mp ⟨_, hr⟩
      refine ⟨?_, ?_⟩
      · intro x hx
        simp only [List.mem_cons] at hx
        rcases hx with rfl | hx
        · exact ⟨hn, by simp [hl]⟩
        · exact h1 x hx
      · intro hs0 val hval
        unfold firstContentLength at hval
        split at hval
        · next hc => simp at hval; subst hval; simp_all
        · next hc => exact h2 (by simp [hs0, hc]) val hval
    · rintro ⟨h1, h2⟩
      have hn := (h1 hd (List.mem_cons_self ..)).1
      have hl := (h1 hd (List.mem_cons_self ..)).2
      obtain ⟨r, hr⟩ := ih.mpr ⟨fun x hx => h1 x (List.mem_cons_of_mem _ hx), fun hs0 val hval => by
        have hc : ¬hd.name = contentLengthName := by intro hc; simp [hc] at hs0
        have : seen = false := by simpa using (Bool.or_eq_false_iff.mp hs0).1
        exact h2 this val (by simp [firstContentLength, hc, hval])⟩
      have hcl : ¬(hd.name = contentLengthName ∧ seen = false ∧ clValid hd.value n = false) := by
        rintro ⟨hc, hs0, hv⟩
        have := h2 hs0 hd.value (by simp [firstContentLength, hc])
        simp [this] at hv
      obtain ⟨line, hline⟩ := Option.isSome_iff_exists.mp hl
      obtain ⟨rest, seen'⟩ := r
      exact ⟨(line ++ lf :: rest, seen'), by simp [hn, hcl, hline, hr]⟩

/-- `ValidFrame` is exactly when the encoder succeeds. -/
theorem encode_ok_iff (v : Version) (f : Frame) : (∃ b, encode v f = .ok b) ↔ ValidFrame v f := by
  obtain ⟨cmd, hs, body⟩ := f
  simp only [encode]
  constructor
  · rintro ⟨b, h⟩
    split at h; · simp at h
    next hbody =>
    split at h; · simp at h
    next hb seen' he =>
    split at h; · simp at h
    next hnul =>
    obtain ⟨h1, h2⟩ := encodeHeaders_ok_iff.mp ⟨_, he⟩
    have hseen := encodeHeaders_seen he
    refine ⟨fun hne => ?_, fun x hx => (h1 x hx).1, fun hesc x hx => ?_, ?_⟩
    · cases hc : cmd.bodyAllowed
      · exact absurd ⟨hne, hc⟩ hbody
      · rfl
    · exact headerLine_isSome.mp (h1 x hx).2 hesc
    · simp only
      split
      · next val hval => exact h2 rfl val hval
      · next hval => simp [hval] at hseen; subst hseen; simpa using hnul
  · rintro ⟨hbody, hnames, hrep, hcl⟩
    simp only at hbody hnames hrep hcl
    obtain ⟨⟨hb, seen'⟩, he⟩ := encodeHeaders_ok_iff (seen := false).mpr
      ⟨fun x hx => ⟨hnames x hx, headerLine_isSome.mpr fun hesc => hrep hesc x hx⟩,
       fun _ val hval => by simpa [hval] using hcl⟩
    have hseen := encodeHeaders_seen he
    have hb1 : ¬(body ≠ [] ∧ cmd.bodyAllowed = false) := by
      rintro ⟨hne, hc⟩; simp [hbody hne] at hc
    have hn : ¬(seen' = false ∧ nul ∈ body) := by
      rintro ⟨hs0, hm⟩
      cases hf : firstContentLength hs with
      | none => simp [hf] at hcl; exact hcl hm
      | some val => simp [hf] at hseen; simp [hseen] at hs0
    exact ⟨cmd.name ++ lf :: hb ++ lf :: body ++ [nul], by simp only [hb1, ite_false, he, hn]⟩

/-! ## Decoding an encoded frame (FRM-2, FRM-5, FRM-6) -/

/-- A header line `a:b` whose parts cannot end the line or, in `a`, hold the
separator. -/
theorem headerLine_parts {v : Version} {a b : Bytes} (ha : colon ∉ a) (hlf : lf ∉ a ∧ lf ∉ b)
    (hcr : v = .v12 → cr ∉ a ∧ cr ∉ b) :
    lf ∉ a ++ colon :: b ∧ blankLine v (a ++ colon :: b) = false ∧
      trimCR v (a ++ colon :: b) = a ++ colon :: b ∧
      splitOn? colon (a ++ colon :: b) = some (a, b) := by
  refine ⟨?_, ?_, ?_, splitOn?_append ha b⟩
  · simp only [List.mem_append, List.mem_cons, not_or]
    exact ⟨hlf.1, by decide, hlf.2⟩
  · unfold blankLine
    cases a <;> simp
    cases b <;> simp [colon, cr]
  · unfold trimCR
    split
    · next h =>
      obtain ⟨hv, hl⟩ := h
      have hmem := List.mem_of_getLast? hl
      simp only [List.mem_append, List.mem_cons] at hmem
      obtain ⟨h1, h2⟩ := hcr hv
      rcases hmem with h | h | h
      · exact absurd h h1
      · exact absurd h (by decide)
      · exact absurd h h2
    · rfl

theorem rawOK_mem {v : Version} {isName : Bool} {s : Bytes} (h : rawOK v isName s = true) :
    lf ∉ s ∧ (v = .v12 → cr ∉ s) ∧ (isName = true → colon ∉ s) := by
  unfold rawOK at h
  rw [List.all_eq_true] at h
  refine ⟨fun hm => ?_, fun hv hm => ?_, fun hn hm => ?_⟩
  · have := h _ hm; simp at this
  · have := h _ hm; simp [hv] at this
  · have := h _ hm; simp [hn] at this

theorem escape_ne_nil {v : Version} {s : Bytes} (h : s ≠ []) : escape v s ≠ [] := by
  cases s with
  | nil => exact absurd rfl h
  | cons c s =>
    unfold escape escapeByte
    simp only [List.flatMap_cons]
    split <;> (try split) <;> (try split) <;> (try split) <;> simp

theorem headerLine_decode {v : Version} {esc : Bool} {h : Header} {line : Bytes}
    (hl : headerLine v esc h = some line) (hn : h.name ≠ []) :
    ∃ a b, lf ∉ line ∧ blankLine v line = false ∧ trimCR v line = line ∧
      splitOn? colon line = some (a, b) ∧ a ≠ [] ∧
      unescapeIf esc v a = .ok h.name ∧ unescapeIf esc v b = .ok h.value := by
  unfold headerLine at hl
  split at hl
  · next hesc =>
    simp at hl; subst hl
    have sa := escape_no_special v h.name
    have sb := escape_no_special v h.value
    obtain ⟨p1, p2, p3, p4⟩ := headerLine_parts (v := v)
      (a := escape v h.name) (b := escape v h.value)
      (fun hm => (sa _ hm).2.1 rfl) ⟨fun hm => (sa _ hm).1 rfl, fun hm => (sb _ hm).1 rfl⟩
      (fun hv => ⟨fun hm => (sa _ hm).2.2 hv rfl, fun hm => (sb _ hm).2.2 hv rfl⟩)
    exact ⟨_, _, p1, p2, p3, p4, escape_ne_nil hn, by simp [unescapeIf, hesc, unescape_escape],
      by simp [unescapeIf, hesc, unescape_escape]⟩
  · next hesc =>
    split at hl
    · next hraw =>
      simp at hl hraw; subst hl
      obtain ⟨a1, a2, a3⟩ := rawOK_mem hraw.1
      obtain ⟨b1, b2, -⟩ := rawOK_mem hraw.2
      obtain ⟨p1, p2, p3, p4⟩ := headerLine_parts (v := v) (a := h.name) (b := h.value)
        (a3 rfl) ⟨a1, b1⟩ (fun hv => ⟨a2 hv, b2 hv⟩)
      exact ⟨_, _, p1, p2, p3, p4, hn, by simp [unescapeIf, hesc], by simp [unescapeIf, hesc]⟩
    · simp at hl

theorem parseLength_of_clValid {val : Bytes} {n : Nat} (h : clValid val n = true) :
    parseLength val = some n := by
  unfold clValid at h
  split at h
  · next k hk => simp at h; rw [hk, h.1]
  · simp at h

theorem encodeHeaders_decode {v : Version} {esc : Bool} {n : Nat} :
    ∀ {hs : List Header} {seen : Bool} {hb : Bytes} {seen' : Bool},
      encodeHeaders v esc n seen hs = .ok (hb, seen') →
        ∃ lines, (∀ tail, headerLines v (hb ++ lf :: tail) = some (lines, tail)) ∧
          ∀ cl0 : Option Nat, cl0.isSome = seen →
            parseHeaders v esc lines cl0 = .ok (hs, if seen' then some (cl0.getD n) else none)
  | [], seen, hb, seen', h => by
    simp [encodeHeaders] at h; obtain ⟨rfl, rfl⟩ := h
    refine ⟨[], fun tail => ?_, fun cl0 hcl => ?_⟩
    · rw [headerLines]; simp [splitOn?, blankLine]
    · cases cl0 <;> simp_all [parseHeaders]
  | hd :: hs, seen, hb, seen', h => by
    simp only [encodeHeaders] at h
    split at h; · simp at h
    next hn =>
    split at h; · simp at h
    next hcl =>
    split at h; · simp at h
    next line hl =>
    split at h; · simp at h
    next rest seen1 hr =>
    simp at h; obtain ⟨rfl, rfl⟩ := h
    obtain ⟨lines, hlines, hparse⟩ := encodeHeaders_decode hr
    obtain ⟨a, b, l1, l2, l3, l4, l5, l6, l7⟩ := headerLine_decode hl hn
    refine ⟨line :: lines, fun tail => ?_, fun cl0 hcl0 => ?_⟩
    · rw [headerLines]
      split
      · next hs0 =>
        simp only [List.append_assoc, List.cons_append] at hs0
        rw [splitOn?_append l1] at hs0; simp at hs0
      · next line' rest' hs0 =>
        simp only [List.append_assoc, List.cons_append] at hs0
        rw [splitOn?_append l1] at hs0
        simp only [Option.some.injEq, Prod.mk.injEq] at hs0
        obtain ⟨rfl, rfl⟩ := hs0
        simp [l2, hlines]
    · rw [parseHeaders.eq_2, l3, l4]
      simp only [l5, ite_false, l6, l7]
      cases cl0 with
      | some k =>
        simp at hcl0; subst hcl0
        simp only
        rw [hparse (some k) (by simp)]
      | none =>
        simp at hcl0; subst hcl0
        simp only
        by_cases hc : hd.name = contentLengthName
        · have hvalid : clValid hd.value n = true := by
            cases hv : clValid hd.value n
            · exact absurd ⟨hc, rfl, hv⟩ hcl
            · rfl
          simp only [hc, ite_true, parseLength_of_clValid hvalid]
          rw [hparse (some n) (by simp [hc])]
          have := encodeHeaders_seen hr
          cases hd; simp_all
        · simp only [hc, ite_false]
          rw [hparse none (by simp [hc])]

theorem readBody_some (body rest : Bytes) :
    readBody (some body.length) (body ++ nul :: rest) = .ok (body, rest) := by
  simp [readBody]

theorem readBody_none {body : Bytes} (h : nul ∉ body) (rest : Bytes) :
    readBody none (body ++ nul :: rest) = .ok (body, rest) := by
  simp [readBody, splitOn?_append h]

theorem skipEOL_command (v : Version) (c : Command) (r : Bytes) :
    skipEOL v (c.name ++ r) = .ok (c.name ++ r) := by
  cases c <;> rw [skipEOL.eq_def] <;> simp [Command.name, lf, cr]

theorem lookupCommand_trimCR (v : Version) (c : Command) : lookupCommand (trimCR v c.name) = some c := by
  cases v <;> cases c <;> decide

theorem lf_not_mem_name (c : Command) : lf ∉ c.name := by
  cases c <;> decide

/-- FRM-2, FRM-5, FRM-6: decoding the encoding of a frame, followed by any
octets, yields that frame and exactly those octets. The hypothesis is that the
encoder succeeds, which by `encode_ok_iff` holds exactly for `ValidFrame`. -/
theorem decode_encode {v : Version} {f : Frame} {b : Bytes} (h : encode v f = .ok b) (rest : Bytes) :
    decodeFrame v (b ++ rest) = .ok (f, rest) := by
  obtain ⟨cmd, hs, body⟩ := f
  simp only [encode] at h
  split at h; · simp at h
  next hbody =>
  split at h; · simp at h
  next hb seen' he =>
  split at h; · simp at h
  next hnul =>
  simp at h; subst h
  obtain ⟨lines, hlines, hparse⟩ := encodeHeaders_decode he
  have hp := hparse none rfl
  simp only [Option.getD_none] at hp
  unfold decodeFrame
  simp only [List.append_assoc, List.cons_append, skipEOL_command]
  rw [splitOn?_append (lf_not_mem_name cmd)]
  simp only
  have := hlines (body ++ nul :: rest)
  simp only [List.nil_append]
  rw [this]
  simp only [lookupCommand_trimCR, hp]
  have hread : readBody (if seen' = true then some body.length else none) (body ++ nul :: rest) =
      .ok (body, rest) := by
    cases seen'
    · exact readBody_none (by simpa using hnul) rest
    · exact readBody_some body rest
  rw [hread]
  simp only
  simp only [hbody, ite_false]

/-- FRM-2, FRM-5, FRM-6: every valid frame encodes, and its encoding followed by
any octets decodes to that frame and exactly those octets. -/
theorem decode_encode_valid {v : Version} {f : Frame} (hv : ValidFrame v f) (rest : Bytes) :
    ∃ b, encode v f = .ok b ∧ decodeFrame v (b ++ rest) = .ok (f, rest) := by
  obtain ⟨b, hb⟩ := (encode_ok_iff v f).mpr hv
  exact ⟨b, hb, decode_encode hb rest⟩

end Stomp
