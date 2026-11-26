/-! The ceiling of `backoffDelay` (client.go), on naturals. The jitter factor
and the float conversion are not modelled. For `base ≥ 1` (`stomp.Backoff`
rejects smaller values) and `max < 2⁶³` (`time.Duration` is an `int64`), Go's
`max>>n` is `max / 2ⁿ`, 0 from `n = 63` on, and `base<<n` is evaluated only when
`base·2ⁿ ≤ max`, so it does not overflow: the naturals compute the same ceiling. -/

namespace Stomp

def backoffCeil (n base max : Nat) : Nat :=
  if base ≤ max >>> n then base <<< n else max

end Stomp
