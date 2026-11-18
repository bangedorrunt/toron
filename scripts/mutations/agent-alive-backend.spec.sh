# Sourced by scripts/verify-mutations.sh.
# bd-1hkt0: liveness must come from the plane that owns the pane. Each impl
# gets a mutant that restores a confident-wrong answer.

CARGO_ARGS=(-p toron-plane --lib)
TEST_FILTER=(hc_liveness_sees_the_agent_binary rex_liveness_distinguishes_a_live_agent_from_its_dead_label)

HC=crates/toron-plane/src/backend/hc.rs
REX=crates/toron-plane/src/backend/superlogical.rs

mutation "L1  hc calls a live agent dead" \
    file=$HC \
    expect=hc_liveness_sees_the_agent_binary \
    find=$'    if binary_hit {\n        return Some(true);\n    }' \
    replace=$'    if binary_hit {\n        return Some(false);\n    }'

mutation "L2  rex reads a retired shell label as inconclusive" \
    file=$REX \
    expect=rex_liveness_distinguishes_a_live_agent_from_its_dead_label \
    find=$'    if ["sh", "bash", "zsh", "fish", "dash"].contains(&foreground) {\n        return Some(false);\n    }' \
    replace=$'    if false {\n        return Some(false);\n    }'
