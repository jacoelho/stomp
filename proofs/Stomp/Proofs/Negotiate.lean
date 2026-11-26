import Stomp.Negotiate
import Stomp.Proofs.Bytes

namespace Stomp

/-- A CONNECTED without `heart-beat` negotiates no heart-beats (CON-9). -/
theorem negotiate_absent (send expect : Int) : negotiate send expect none = some ⟨0, 0⟩ := rfl

/-- `negotiate` accepts exactly a header `sx,sy` of two decimal numbers below 2³¹;
the client sends at the interval negotiated from its `send` and the server's
`sy`, and expects at the one from its `expect` and the server's `sx`. -/
theorem negotiate_some_iff (send expect : Int) (header : Bytes) (hb : Heartbeat) :
    negotiate send expect (some header) = some hb ↔
      ∃ sx sy x y, comma ∉ sx ∧ header = sx ++ comma :: sy ∧
        parseUint31 sx = some x ∧ parseUint31 sy = some y ∧
        hb = ⟨pick send (y * 1000000), pick expect (x * 1000000)⟩ := by
  simp only [negotiate]
  constructor
  · intro h
    split at h
    · simp at h
    · next sx sy hsplit =>
      obtain ⟨hx, rfl⟩ := splitOn?_eq_some_iff.mp hsplit
      split at h
      · next x y h1 h2 => simp at h; exact ⟨sx, sy, x, y, hx, rfl, h1, h2, h.symm⟩
      · simp at h
  · rintro ⟨sx, sy, x, y, hx, rfl, h1, h2, rfl⟩
    rw [splitOn?_append hx]
    simp [h1, h2]

/-- Each negotiated direction is 0 when either side offers 0 (CON-7). -/
theorem pick_eq_zero {mine theirs : Int} (h : mine = 0 ∨ theirs = 0) : pick mine theirs = 0 := by
  simp [pick, h]

/-- Otherwise it is the larger of the two values (CON-7). -/
theorem pick_eq_max {mine theirs : Int} (h1 : mine ≠ 0) (h2 : theirs ≠ 0) :
    pick mine theirs = max mine theirs := by
  simp [pick, h1, h2]

/-- CON-7: each negotiated direction is 0 when either side offers 0, and
otherwise the larger of the two offers. The client's send interval pairs with
the server's `sy` and its expected interval with the server's `sx`, where the
header is `sx,sy` in milliseconds. -/
theorem negotiate_directions {send expect : Int} {header : Bytes} {hb : Heartbeat}
    (h : negotiate send expect (some header) = some hb) :
    ∃ sx sy x y, header = sx ++ comma :: sy ∧ parseUint31 sx = some x ∧ parseUint31 sy = some y ∧
      (send = 0 ∨ y = 0 → hb.send = 0) ∧ (send ≠ 0 → y ≠ 0 → hb.send = max send (y * 1000000)) ∧
      (expect = 0 ∨ x = 0 → hb.recv = 0) ∧ (expect ≠ 0 → x ≠ 0 → hb.recv = max expect (x * 1000000)) := by
  obtain ⟨sx, sy, x, y, -, rfl, hx, hy, rfl⟩ := (negotiate_some_iff send expect header hb).mp h
  refine ⟨sx, sy, x, y, rfl, hx, hy, fun h => pick_eq_zero ?_, fun h1 h2 => pick_eq_max h1 ?_,
    fun h => pick_eq_zero ?_, fun h1 h2 => pick_eq_max h1 ?_⟩ <;> omega

end Stomp
