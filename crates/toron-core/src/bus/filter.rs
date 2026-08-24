// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Filter model mirroring NIP-01 (authors, kinds, since, until, limit, #e,
//! #p, #t, #l) plus toron project-scoping helpers.
//!
//! The underlying [`Filter`] is rust-nostr's — wire-identical semantics with a
//! real relay. toron adds constructors for the ADR-0003 scoping rules:
//! InProcessBus uses `#t=[slug]`; relay-crossing kinds additionally carry
//! NIP-32 labels `["l", slug, "toron"]` (`#l`).

use crate::nostr::{Kind, PublicKey, SingleLetterTag, Timestamp};

/// The NIP-32 label namespace toron uses (ADR-0003 A3).
pub const LABEL_NAMESPACE: &str = "toron";

/// Re-export of the underlying NIP-01 filter type.
pub use crate::nostr::Filter;

/// A label value in NIP-32 `["l", <value>, "toron"]` form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelValue {
    /// The `l` value (e.g. the project slug).
    pub value: String,
    /// The label namespace (always `"toron"`).
    pub namespace: String,
}

impl LabelValue {
    /// New toron-namespaced label value.
    pub fn toron(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            namespace: LABEL_NAMESPACE.to_string(),
        }
    }
}

/// Project-scoped filter over the InProcessBus `#t` shortcut.
///
/// Matches any event carrying `["t", <slug>]`. This is the cheap in-process
/// index (ADR-0003 G6/A3); relay-crossing kinds use [`project_label_filter`]
/// instead.
pub fn project_filter(slug: &str) -> Filter {
    Filter::new().hashtag(slug)
}

/// Project-scoped filter over NIP-32 labels (`#l`).
///
/// Matches `["l", <slug>, "toron"]` events (the tag content at position 1).
/// Use for relay-crossing queries and federation, where bare `#t` collides
/// with other applications (ADR-0003 A3).
pub fn project_label_filter(slug: &str) -> Filter {
    let tag = SingleLetterTag::LOWERCASE_L;
    Filter::new().custom_tag(tag, slug)
}

/// Filter by kind + project slug (`#t`).
pub fn kind_project_filter(kind: Kind, slug: &str) -> Filter {
    project_filter(slug).kinds([kind])
}

/// Filter by authors + project slug (`#t`).
pub fn authors_project_filter(authors: impl IntoIterator<Item = PublicKey>, slug: &str) -> Filter {
    project_filter(slug).authors(authors)
}

/// A NIP-01-style filter builder guard for common toron queries.
#[derive(Debug, Clone, Default)]
pub struct ToronFilter {
    inner: Filter,
}

impl ToronFilter {
    /// Empty filter (matches everything).
    pub fn new() -> Self {
        Self {
            inner: Filter::new(),
        }
    }

    /// Add authors.
    pub fn authors(mut self, authors: impl IntoIterator<Item = PublicKey>) -> Self {
        self.inner = self.inner.authors(authors);
        self
    }

    /// Add kinds.
    pub fn kinds(mut self, kinds: impl IntoIterator<Item = Kind>) -> Self {
        self.inner = self.inner.kinds(kinds);
        self
    }

    /// Add `#e` event reference.
    pub fn event(mut self, id: crate::nostr::EventId) -> Self {
        self.inner = self.inner.event(id);
        self
    }

    /// Add `#p` pubkey reference.
    pub fn pubkey(mut self, pubkey: PublicKey) -> Self {
        self.inner = self.inner.pubkey(pubkey);
        self
    }

    /// Add `#t` project shortcut.
    pub fn project(mut self, slug: &str) -> Self {
        self.inner = self.inner.hashtag(slug);
        self
    }

    /// Add NIP-32 `#l` label.
    pub fn label(mut self, label: &LabelValue) -> Self {
        self.inner = self
            .inner
            .custom_tag(SingleLetterTag::LOWERCASE_L, label.value.clone());
        self
    }

    /// Lower bound (inclusive).
    pub fn since(mut self, since: Timestamp) -> Self {
        self.inner = self.inner.since(since);
        self
    }

    /// Upper bound (inclusive).
    pub fn until(mut self, until: Timestamp) -> Self {
        self.inner = self.inner.until(until);
        self
    }

    /// Maximum number of stored events to replay.
    pub fn limit(mut self, limit: usize) -> Self {
        self.inner = self.inner.limit(limit);
        self
    }

    /// The underlying rust-nostr filter.
    pub fn into_inner(self) -> Filter {
        self.inner
    }

    /// Build + consume.
    pub fn build(self) -> Filter {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::event::build_event;
    use crate::nostr::kinds::{AGENT_PROFILE, PROJECT_MANIFEST};
    use crate::nostr::{Keys, Kind, Tag, Timestamp};

    fn keys() -> Keys {
        Keys::generate()
    }

    fn ev_with_t(keys: &Keys, slug: &str) -> crate::nostr::Event {
        build_event(
            keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "m",
            vec![Tag::parse(["t", slug]).expect("t")],
            Timestamp::now(),
        )
        .expect("build")
    }

    fn ev_with_label(keys: &Keys, slug: &str) -> crate::nostr::Event {
        build_event(
            keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "m",
            vec![
                Tag::parse(["L", "toron"]).expect("L"),
                Tag::parse(["l", slug, "toron"]).expect("l"),
            ],
            Timestamp::now(),
        )
        .expect("build")
    }

    #[test]
    fn test_project_filter_matches_hashtag() {
        let k = keys();
        let ev = ev_with_t(&k, "demo");
        let f = project_filter("demo");
        assert!(f.match_event(&ev, Default::default()));
        assert!(!project_filter("other").match_event(&ev, Default::default()));
    }

    #[test]
    fn test_project_label_filter_matches_label() {
        let k = keys();
        let ev = ev_with_label(&k, "demo");
        let f = project_label_filter("demo");
        assert!(f.match_event(&ev, Default::default()));
        assert!(!project_label_filter("other").match_event(&ev, Default::default()));
    }

    #[test]
    fn test_kind_project_filter() {
        let k = keys();
        let ev = ev_with_t(&k, "demo");
        assert!(
            kind_project_filter(crate::nostr::kinds::toron_kind(PROJECT_MANIFEST), "demo")
                .match_event(&ev, Default::default())
        );
        // Wrong kind → no match.
        assert!(
            !kind_project_filter(crate::nostr::kinds::toron_kind(AGENT_PROFILE), "demo")
                .match_event(&ev, Default::default())
        );
    }

    #[test]
    fn test_authors_project_filter() {
        let k = keys();
        let ev = ev_with_t(&k, "demo");
        let other = Keys::generate();
        assert!(
            authors_project_filter([k.public_key()], "demo").match_event(&ev, Default::default())
        );
        assert!(
            !authors_project_filter([other.public_key()], "demo")
                .match_event(&ev, Default::default())
        );
    }

    #[test]
    fn test_toron_filter_builder() {
        let k = keys();
        let ev = ev_with_t(&k, "demo");
        let f = ToronFilter::new()
            .project("demo")
            .kinds([crate::nostr::kinds::toron_kind(PROJECT_MANIFEST)])
            .build();
        assert!(f.match_event(&ev, Default::default()));

        let f_no_kind = ToronFilter::new()
            .project("demo")
            .kinds([Kind::TextNote])
            .build();
        assert!(!f_no_kind.match_event(&ev, Default::default()));
    }

    #[test]
    fn test_label_value_always_toron_namespace() {
        assert_eq!(
            LabelValue::toron("demo"),
            LabelValue {
                value: "demo".into(),
                namespace: "toron".into()
            }
        );
    }
}
