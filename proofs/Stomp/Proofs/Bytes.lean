import Stomp.Frame

namespace Stomp

theorem splitOn?_eq_some_iff {b : UInt8} :
    ∀ {l x y : Bytes}, splitOn? b l = some (x, y) ↔ b ∉ x ∧ l = x ++ b :: y
  | [], x, y => by simp [splitOn?]
  | c :: r, x, y => by
    by_cases hc : c = b
    · subst hc
      simp only [splitOn?, ite_true, Option.some.injEq, Prod.mk.injEq]
      constructor
      · rintro ⟨rfl, rfl⟩; simp
      · rintro ⟨hx, he⟩
        cases x with
        | nil => simp at he; exact ⟨rfl, he⟩
        | cons d x => simp at he hx; exact absurd he.1 hx.1
    · simp only [splitOn?, hc, ite_false]
      cases x with
      | nil => cases e : splitOn? b r <;> simp [hc]
      | cons d x =>
        cases e : splitOn? b r with
        | none =>
          simp only [Option.map_none, reduceCtorEq, false_iff]
          rintro ⟨hx, he⟩
          simp at he
          have := (splitOn?_eq_some_iff (b := b) (l := r) (x := x) (y := y)).mpr ⟨fun h => hx (List.mem_cons_of_mem _ h), he.2⟩
          simp [e] at this
        | some p =>
          obtain ⟨p1, p2⟩ := p
          have ih := splitOn?_eq_some_iff (b := b) (l := r) (x := p1) (y := p2)
          simp only [e, true_iff] at ih
          simp only [Option.map_some, Option.some.injEq, Prod.mk.injEq, List.cons.injEq,
            List.mem_cons, List.cons_append, not_or]
          constructor
          · rintro ⟨⟨rfl, rfl⟩, rfl⟩
            exact ⟨⟨fun h => hc h.symm, ih.1⟩, rfl, ih.2⟩
          · rintro ⟨⟨hd, hx⟩, rfl, he⟩
            have := (splitOn?_eq_some_iff (b := b) (l := r) (x := x) (y := y)).mpr ⟨hx, he⟩
            rw [e] at this; simp at this
            exact ⟨⟨rfl, this.1⟩, this.2⟩

theorem splitOn?_append {b : UInt8} {x : Bytes} (hx : b ∉ x) (y : Bytes) :
    splitOn? b (x ++ b :: y) = some (x, y) :=
  splitOn?_eq_some_iff.mpr ⟨hx, rfl⟩

theorem splitOn?_eq_none {b : UInt8} : ∀ {l : Bytes}, b ∉ l → splitOn? b l = none
  | [], _ => rfl
  | c :: r, h => by
    simp at h
    simp [splitOn?, Ne.symm h.1, splitOn?_eq_none h.2]

end Stomp
