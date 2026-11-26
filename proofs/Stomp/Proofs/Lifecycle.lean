import Stomp.Lifecycle

namespace Stomp.Lifecycle

/-- The lifecycle never moves back: running, then closing, then closed. -/
theorem step_life_forward {s s' : State} {e : Event} (h : step s e = some s') :
    s.life.rank ≤ s'.life.rank := by
  cases e <;> simp only [step] at h <;> (repeat' split at h) <;> simp at h <;>
    (try subst h) <;> simp_all <;> (cases s.life <;> simp_all [Life.rank])

/-- The facts every reachable state keeps. -/
def Inv (s : State) : Prop :=
  (s.life = .running → s.reported = .connected → s.session = true) ∧
  (s.handshook = true → s.session = false) ∧
  (s.life = .closed → s.session = false ∧ s.handshook = false ∧ s.reported = .closed)

theorem inv_init : Inv init := by simp [Inv, init]

theorem inv_step {s s' : State} {e : Event} (hi : Inv s) (h : step s e = some s') : Inv s' := by
  obtain ⟨h1, h2, h3⟩ := hi
  cases e <;> simp only [step] at h <;> (repeat' split at h) <;> simp at h <;> subst h <;>
    simp_all [Inv] <;>
    (try (cases hl : s.life <;> simp_all)) <;>
    (try (cases hs : s.session <;> simp_all)) <;>
    (try (cases hr : s.reported <;> simp_all))

theorem reachable_inv {s : State} (h : Reachable s) : Inv s := by
  induction h with
  | init => exact inv_init
  | step e _ hs ih => exact inv_step ih hs

/-- RCN-15: while the Client runs, it reports Connected only when a session
runs. -/
theorem connected_means_session {s : State} (h : Reachable s)
    (hl : s.life = .running) (hr : s.reported = .connected) : s.session = true :=
  (reachable_inv h).1 hl hr

/-- CON-30: once Close began, no event starts a session. -/
theorem no_session_after_close {s s' : State} {e : Event}
    (hl : s.life ≠ .running) (h : step s e = some s') (hs : s'.session = true) :
    s.session = true := by
  cases e <;> simp only [step] at h <;> (repeat' split at h) <;> simp at h <;> subst h <;>
    simp_all

/-- RCN-16: once Close began, the reported state changes only to Closed. -/
theorem state_frozen_after_close {s s' : State} {e : Event}
    (hl : s.life ≠ .running) (h : step s e = some s') :
    s'.reported = s.reported ∨ s'.reported = .closed := by
  cases e <;> simp only [step] at h <;> (repeat' split at h) <;> simp at h <;> subst h <;>
    simp_all

/-- Closed is final: no event changes a closed Client, and it holds no
session or handshake and reports Closed. -/
theorem closed_is_final {s s' : State} {e : Event} (h : Reachable s) (hl : s.life = .closed)
    (hs : step s e = some s') : s' = s := by
  obtain ⟨-, -, h3⟩ := reachable_inv h
  obtain ⟨hn, hh, -⟩ := h3 hl
  cases e <;> simp only [step] at hs <;> (repeat' split at hs) <;> simp at hs <;> simp_all

end Stomp.Lifecycle
