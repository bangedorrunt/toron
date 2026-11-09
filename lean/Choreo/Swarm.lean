/-
  governed-by: ADR-0041
  `G_SWARM`: captain tasks a crew; the crew progresses, finishes, or rejects;
  a verifier verdicts; the captain asks for approval. Par, so state 0 unions
  them per role.
-/
import Choreo

namespace Choreo

def swarmCrew : List Global := [
  .send .crew .captain "progress",
  .send .crew .captain "done",
  .send .crew .captain "reject",
]

def gSwarm : Global := .par [
  .send .captain .crew "task",
  .choice swarmCrew,
  .send .verifier .captain "verdict",
  .send .captain .captain "approval",
]

end Choreo
