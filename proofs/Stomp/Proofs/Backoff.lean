import Stomp.Backoff

namespace Stomp

/-- RCN-7: the ceiling of backoff attempt `n` is `min(max, base·2ⁿ)` at every
attempt. The statement needs no hypothesis on naturals; the module comment of
`Stomp.Backoff` gives the range (`base ≥ 1`, `max < 2⁶³`) where the naturals
compute what Go computes. -/
theorem backoffCeil_eq_min (n base max : Nat) : backoffCeil n base max = min max (base * 2 ^ n) := by
  unfold backoffCeil
  simp only [Nat.shiftLeft_eq, Nat.shiftRight_eq_div_pow, Nat.le_div_iff_mul_le (Nat.two_pow_pos n)]
  split <;> omega

end Stomp
