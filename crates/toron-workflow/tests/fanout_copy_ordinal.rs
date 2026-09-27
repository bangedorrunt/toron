// governed-by: ADR-0007
//
// bd-0s0y: the fan-out copy ordinal is the one thing that lets a `parallel`
// fan-out reach N different agents. If it cannot be read back off a leaf's
// `iteration_key`, every copy resolves to the same agent and the busy-owner
// fence refuses copies 2..N — which is why `steer --crew` never completed.
//
// These pin the READ side. The end-to-end proof (three copies landing on
// three panes) lives in flywheel as
// `pane::tests::fanout_copies_reach_distinct_panes`, because that is where
// the role→pane resolution actually is.

use toron_workflow::identity::copy_ordinal;

#[test]
fn a_parallel_copy_key_reads_back_as_its_ordinal() {
    // run_parallel mints exactly this form, one per copy.
    assert_eq!(copy_ordinal(Some("p0")), Some(0));
    assert_eq!(copy_ordinal(Some("p1")), Some(1));
    assert_eq!(copy_ordinal(Some("p2")), Some(2));
    assert_eq!(copy_ordinal(Some("p11")), Some(11));
}

#[test]
fn a_non_fanout_key_has_no_ordinal() {
    // A step outside a `parallel` carries no key at all: ordinal-blind
    // resolution, which is the pre-existing and correct behavior.
    assert_eq!(copy_ordinal(None), None);
}

#[test]
fn only_the_p_digit_form_parses() {
    // Strict by design. A lenient parse would let an unrelated key choose a
    // pane, so every near-miss must be None rather than Some(0).
    for key in [
        "p",       // prefix only
        "px",      // non-digit
        "p-1",     // signed
        "p 1",     // whitespace
        "p1 ",     // trailing space
        "P1",      // wrong case
        "pp1",     // doubled prefix
        "1",       // no prefix
        "",        // empty
        "node-p1", // embedded
    ] {
        assert_eq!(copy_ordinal(Some(key)), None, "{key:?} must not parse");
    }
}

#[test]
fn an_effect_key_digest_is_never_mistaken_for_an_ordinal() {
    // identity::iteration_key mints a sha256 digest for EFFECT keys. It must
    // never reach RunContext.iteration_key, and if it somehow did it must not
    // parse as a copy ordinal.
    let digest = toron_workflow::identity::iteration_key("parent-digest", 0);
    assert_eq!(digest.len(), 64, "sha256 hex, not a p<i> form");
    assert_eq!(copy_ordinal(Some(&digest)), None);
}

#[test]
fn an_index_too_large_for_usize_is_none_not_a_wrapped_ordinal() {
    // Wrapping would address a real but wrong pane, so overflow is None.
    assert_eq!(copy_ordinal(Some("p99999999999999999999999999")), None);
}
