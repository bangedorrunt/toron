import Lake
open Lake DSL

-- governed-by: ADR-0041. Executable reference model of the comms protocol
-- (redesign §10). No Mathlib: build time stays small.
package proto where

lean_lib Proto where

lean_lib Choreo where

@[default_target]
lean_exe proto where
  root := `Main
