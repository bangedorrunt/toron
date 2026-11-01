# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# hc backend + OSC fold (feat/hc-backend): Attention priority, error→Unknown
# fence, and hello method-skew refusal.
#
#   scripts/verify-mutations.sh scripts/mutations/hc-osc-fold.spec.sh --cargo cargo

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(attention_prefers_blocked_child_over_working_root attention_maps_error_to_unknown_never_done missing_method_at_hello_is_typed_error)

OSC=crates/toron-core/src/osc7501.rs
HC=crates/toron-core/src/backend/hc.rs

mutation "M1  Attention no longer prefers blocked over working" \
    file=$OSC \
    expect=attention_prefers_blocked_child_over_working_root \
    find='            ProgramState::Blocked => return AgentStatus::Blocked,' \
    replace='            ProgramState::Blocked => saw_working = true,'

mutation "M2  OSC error maps to Done instead of Unknown" \
    file=$OSC \
    expect=attention_maps_error_to_unknown_never_done \
    find=$'    if saw_error {\n        return AgentStatus::Unknown;\n    }' \
    replace=$'    if saw_error {\n        return AgentStatus::Done;\n    }'

mutation "M3  hello skips the missing-method skew check" \
    file=$HC \
    expect=missing_method_at_hello_is_typed_error \
    find=$'            if !hello.methods.iter().any(|m| m == method) {\n                return Err(BackendError::new(\n                    BackendErrorCode::Other(format!("missing_method:{method}")),\n                    format!("hc hello omitted required method {method}"),\n                ));\n            }' \
    replace=$'            let _ = method;'
