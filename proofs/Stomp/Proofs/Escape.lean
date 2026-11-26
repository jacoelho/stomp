import Stomp.Escape

namespace Stomp

theorem unescape_escapeByte_append (v : Version) (c : UInt8) (t : Bytes) :
    unescape v (escapeByte v c ++ t) = (c :: ·) <$> unescape v t := by
  unfold escapeByte
  by_cases h1 : c = backslash
  · subst h1; simp; rfl
  by_cases h2 : c = lf
  · subst h2; simp; rfl
  by_cases h3 : c = colon
  · subst h3; simp; rfl
  by_cases h4 : c = cr ∧ v = .v12
  · obtain ⟨rfl, rfl⟩ := h4; simp; rfl
  · simp only [h1, h2, h3, h4, ite_false, List.cons_append, List.nil_append]
    rw [unescape.eq_def]; simp [h1]

/-- Unescaping inverts escaping in both versions (FRM-10, FRM-11, FRM-31). -/
theorem unescape_escape (v : Version) (s : Bytes) : unescape v (escape v s) = .ok s := by
  induction s with
  | nil => rfl
  | cons c s ih =>
    simp only [escape, List.flatMap_cons] at ih ⊢
    rw [unescape_escapeByte_append, ih]; rfl

/-- Escaped text holds no LF and no colon, and in 1.2 no CR, so it cannot end a
header line or split a name from its value (FRM-11, FRM-31). -/
theorem escape_no_special (v : Version) (s : Bytes) :
    ∀ c ∈ escape v s, c ≠ lf ∧ c ≠ colon ∧ (v = .v12 → c ≠ cr) := by
  intro c hc
  simp only [escape, List.mem_flatMap] at hc
  obtain ⟨a, -, ha⟩ := hc
  unfold escapeByte at ha
  have k : ∀ d : UInt8, d ∈ [backslash, 110, 99, 114] →
      d ≠ lf ∧ d ≠ colon ∧ (v = .v12 → d ≠ cr) := by
    intro d hd
    simp only [List.mem_cons, List.not_mem_nil, or_false] at hd
    rcases hd with rfl | rfl | rfl | rfl <;> exact ⟨by decide, by decide, fun _ => by decide⟩
  split at ha
  · apply k; simp only [List.mem_cons, List.not_mem_nil, or_false] at ha ⊢; rcases ha with h | h <;> simp [h]
  split at ha
  · apply k; simp only [List.mem_cons, List.not_mem_nil, or_false] at ha ⊢; rcases ha with h | h <;> simp [h]
  split at ha
  · apply k; simp only [List.mem_cons, List.not_mem_nil, or_false] at ha ⊢; rcases ha with h | h <;> simp [h]
  split at ha
  · apply k; simp only [List.mem_cons, List.not_mem_nil, or_false] at ha ⊢; rcases ha with h | h <;> simp [h]
  next h1 h2 h3 h4 =>
    simp at ha; subst ha
    exact ⟨h2, h3, fun hv h => h4 ⟨h, hv⟩⟩

/-- In 1.1, `\r` is an undefined escape (FRM-31). -/
theorem unescape_v11_cr_escape (t : Bytes) :
    unescape .v11 (backslash :: 114 :: t) = .error .invalidEscape := by
  simp [unescape, unescapeByte]
  rfl

end Stomp
