/-
  governed-by: ADR-0041
  `G_OPEN`: peers request, reply, and notify. Nobody else sends.
-/
import Choreo

namespace Choreo

def gOpen : Global := .par [
  .send .peer .peer "request",
  .send .peer .peer "reply",
  .send .peer .peer "notify",
]

end Choreo
