/-
  governed-by: ADR-0041
  `G_LOOP`: the loop unwraps to one Par. loop-self ticks, tasks a crew, and
  tells the captain `notify` or `not_ready`. The crew answers `done`.
-/
import Choreo

namespace Choreo

def gLoop : Global := .loop (.par [
  .send .loopSelf .loopSelf "tick",
  .send .loopSelf .crew "task",
  .send .crew .loopSelf "done",
  .send .loopSelf .captain "notify",
  .send .loopSelf .captain "not_ready",
])

end Choreo
