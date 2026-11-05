// governed-by: ADR-0007

use toron_workflow::identity::copy_ordinal;

#[test]
fn a_parallel_copy_key_reads_back_as_its_ordinal() {
    assert_eq!(copy_ordinal(Some("p0")), Some(0));
    assert_eq!(copy_ordinal(Some("p1")), Some(1));
    assert_eq!(copy_ordinal(Some("p2")), Some(2));
    assert_eq!(copy_ordinal(Some("p11")), Some(11));
}

#[test]
fn a_non_fanout_key_has_no_ordinal() {
    assert_eq!(copy_ordinal(None), None);
}

#[test]
fn only_the_p_digit_form_parses() {
    for key in [
        "p",
        "px",
        "p-1",
        "p 1",
        "p1 ",
        "P1",
        "pp1",
        "1",
        "",
        "node-p1",
    ] {
        assert_eq!(copy_ordinal(Some(key)), None, "{key:?} must not parse");
    }
}

#[test]
fn an_effect_key_digest_is_never_mistaken_for_an_ordinal() {
    let digest = toron_workflow::identity::iteration_key("parent-digest", 0);
    assert_eq!(digest.len(), 64, "sha256 hex, not a p<i> form");
    assert_eq!(copy_ordinal(Some(&digest)), None);
}

#[test]
fn an_index_too_large_for_usize_is_none_not_a_wrapped_ordinal() {
    assert_eq!(copy_ordinal(Some("p99999999999999999999999999")), None);
}
