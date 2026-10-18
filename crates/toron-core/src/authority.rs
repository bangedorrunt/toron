// governed-by: ADR-0041
//! Roles and authority at append (redesign §6.1, Phase 4.1).
//!
//! Pure: `std + serde` only, no clock, no store. The store reads a sender's
//! [`Standing`] and the task [`Relation`] and asks this module one question;
//! the answer is `Ok(())` or a typed [`Reject::Unauthorized`] carrying the
//! verbs the sender's role may use instead.
//!
//! * [`Role`] is one of `captain | supervisor | crew | peer | loop_self |
//!   verifier`. A role is a log fact (`role@1`, see
//!   [`crate::contracts::role_v1`]), read per `(context, npub)`.
//! * [`Authority`] is the table `(role, class, contract) -> allowed?`,
//!   copied from §6.1. It is default-deny: an act is allowed only by a
//!   [`Grant`]. `done` is not an envelope; it is the one non-envelope verb the
//!   table names.
//! * A context with no role fact is **ungoverned**: nothing here refuses
//!   anything, so flag-on contexts that never adopt roles behave as before.
//!   The first role fact makes it governed; from then on a sender with no
//!   role (and not the operator) is refused.
//! * The operator key acts with captain authority everywhere, and is the only
//!   key that may start a context (`role{captain}`) or mint a captain.
//! * Two rules need more than `(role, class, contract)`: a supervisor may not
//!   verdict a task it sent ([`Relation::OwnTask`]), and who may issue which
//!   role fact ([`Authority::check_grant`]).

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::delivery::Reject;
use crate::envelope::{Class, ContractId};

/// Contract name of the role fact.
pub const ROLE_CONTRACT: &str = "role";
/// Contract name of the topology fact (`context@1`).
pub const CONTEXT_CONTRACT: &str = "context";
/// Contract name of a verifier's verdict.
pub const VERDICT_CONTRACT: &str = "verdict";
/// The one non-envelope verb the table names (`toron done`).
pub const DONE_VERB: &str = "done";

/// A role in a context (redesign §6.1).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// The human, or the agent holding the captain role. Any class.
    Captain,
    /// Captain, or loop-self acting as one: tasks, steers, cancels.
    Supervisor,
    /// Does the work: acks, progresses, finishes, asks.
    Crew,
    /// Equal of another agent: request, reply, notify. No task, no control.
    Peer,
    /// A loop acting on itself: task (to crew or itself), self-tick.
    LoopSelf,
    /// The gate: verdict only, never `done` on the same task.
    Verifier,
}

impl Role {
    /// Every role, in a fixed order.
    pub const ALL: [Self; 6] = [
        Self::Captain,
        Self::Supervisor,
        Self::Crew,
        Self::Peer,
        Self::LoopSelf,
        Self::Verifier,
    ];

    /// The wire spelling (`snake_case`), as serde writes it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Captain => "captain",
            Self::Supervisor => "supervisor",
            Self::Crew => "crew",
            Self::Peer => "peer",
            Self::LoopSelf => "loop_self",
            Self::Verifier => "verifier",
        }
    }

    /// Parse the wire spelling.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|r| r.as_str() == s)
    }

    /// A role a supervisor may hand out: everything below the supervisor.
    pub const fn is_leaf(self) -> bool {
        matches!(
            self,
            Self::Crew | Self::Peer | Self::LoopSelf | Self::Verifier
        )
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Who a sender is in one context, as the store resolved it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Standing {
    /// The operator key. Acts with captain authority everywhere.
    Operator,
    /// The sender's current role fact.
    Holds(Role),
    /// No role fact for this sender in this context.
    Unroled,
}

impl Standing {
    /// The role this standing acts as (the operator acts as captain).
    pub const fn acts_as(self) -> Option<Role> {
        match self {
            Self::Operator => Some(Role::Captain),
            Self::Holds(r) => Some(r),
            Self::Unroled => None,
        }
    }
}

/// How the sender relates to the task an envelope is about (read from the
/// log by the store; the table cannot see it).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Relation {
    /// The sender did not send the task, or the envelope names no task.
    Other,
    /// The sender sent a `task` envelope for this task.
    OwnTask,
}

/// One row of the table: `role` may send `class`, and either any contract of
/// that class (`contract = None`) or the one named.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Grant {
    /// Who.
    pub role: Role,
    /// Which class.
    pub class: Class,
    /// Which contract name (major-agnostic); `None` = any of the class.
    pub contract: Option<&'static str>,
}

const fn any(role: Role, class: Class) -> Grant {
    Grant {
        role,
        class,
        contract: None,
    }
}

const fn only(role: Role, class: Class, contract: &'static str) -> Grant {
    Grant {
        role,
        class,
        contract: Some(contract),
    }
}

/// The table, redesign §6.1 transcribed. Default-deny: a pair not listed is
/// refused.
pub const STANDARD: &[Grant] = &[
    // captain: any class.
    any(Role::Captain, Class::Notify),
    any(Role::Captain, Class::Request),
    any(Role::Captain, Class::Reply),
    any(Role::Captain, Class::Task),
    any(Role::Captain, Class::Control),
    any(Role::Captain, Class::Receipt),
    // supervisor: task, steer, cancel, notify to its children; replies and
    // requests; never an approval; a verdict only on a task it did not send
    // (the `Relation` rule). `role` is further limited by `check_grant`.
    any(Role::Supervisor, Class::Notify),
    any(Role::Supervisor, Class::Request),
    any(Role::Supervisor, Class::Reply),
    any(Role::Supervisor, Class::Task),
    any(Role::Supervisor, Class::Receipt),
    only(Role::Supervisor, Class::Control, "steer"),
    only(Role::Supervisor, Class::Control, "cancel"),
    only(Role::Supervisor, Class::Control, "goal"),
    only(Role::Supervisor, Class::Control, "escalate"),
    only(Role::Supervisor, Class::Control, VERDICT_CONTRACT),
    only(Role::Supervisor, Class::Control, ROLE_CONTRACT),
    // context@1: the topology fact. Captain already has every Control via
    // the wildcard; a supervisor may set it too (Phase 4.2).
    only(Role::Supervisor, Class::Control, CONTEXT_CONTRACT),
    // crew: reply, request to a peer, notify. No task. The one control
    // they may send is an upward handoff (`escalate@1`).
    any(Role::Crew, Class::Notify),
    any(Role::Crew, Class::Request),
    any(Role::Crew, Class::Reply),
    only(Role::Crew, Class::Control, "escalate"),
    // peer: request, reply, notify.
    any(Role::Peer, Class::Notify),
    any(Role::Peer, Class::Request),
    any(Role::Peer, Class::Reply),
    // loop-self: task (to crew or itself), self-tick, its iteration notice.
    any(Role::LoopSelf, Class::Task),
    any(Role::LoopSelf, Class::Notify),
    any(Role::LoopSelf, Class::Receipt),
    only(Role::LoopSelf, Class::Control, "tick"),
    // verifier: verdict, and nothing else.
    only(Role::Verifier, Class::Control, VERDICT_CONTRACT),
];

/// Roles that may run `done` (the verifier and the peer may not).
pub const STANDARD_DONE: &[Role] = &[Role::Captain, Role::Supervisor, Role::Crew, Role::LoopSelf];

/// The authority table plus the roles allowed `done`. [`Authority::standard`]
/// is the one the store uses; [`Authority::new`] exists so a test can run the
/// same checks against a stub (a negative control).
#[derive(Clone, Copy, Debug)]
pub struct Authority {
    grants: &'static [Grant],
    done: &'static [Role],
}

impl Default for Authority {
    fn default() -> Self {
        Self::standard()
    }
}

/// One case of redesign §6.1 as an independent statement of the table:
/// `(role, class, contract name, allowed)`. [`Authority::deviations`] checks a
/// table against it; a table that allows everything deviates.
pub const SPEC: &[(Role, Class, &str, bool)] = &[
    (Role::Crew, Class::Task, "task", false),
    (Role::Crew, Class::Control, "verdict", false),
    (Role::Crew, Class::Control, "approval", false),
    (Role::Crew, Class::Control, "steer", false),
    (Role::Crew, Class::Control, "escalate", true),
    (Role::Crew, Class::Control, "goal", false),
    (Role::Crew, Class::Control, "cancel", false),
    (Role::Crew, Class::Control, "role", false),
    (Role::Crew, Class::Request, "request", true),
    (Role::Crew, Class::Reply, "reply", true),
    (Role::Crew, Class::Notify, "notify", true),
    (Role::Peer, Class::Task, "task", false),
    (Role::Peer, Class::Control, "steer", false),
    (Role::Peer, Class::Control, "verdict", false),
    (Role::Peer, Class::Control, "cancel", false),
    (Role::Peer, Class::Control, "role", false),
    (Role::Peer, Class::Request, "request", true),
    (Role::Peer, Class::Reply, "reply", true),
    (Role::Peer, Class::Notify, "notify", true),
    (Role::Supervisor, Class::Task, "task", true),
    (Role::Supervisor, Class::Control, "steer", true),
    (Role::Supervisor, Class::Control, "cancel", true),
    (Role::Supervisor, Class::Control, "verdict", true),
    (Role::Supervisor, Class::Control, "role", true),
    (Role::Supervisor, Class::Control, "context", true),
    (Role::Crew, Class::Control, "context", false),
    (Role::Peer, Class::Control, "context", false),
    (Role::LoopSelf, Class::Control, "context", false),
    (Role::Verifier, Class::Control, "context", false),
    (Role::Captain, Class::Control, "context", true),
    (Role::Supervisor, Class::Notify, "notify", true),
    (Role::Supervisor, Class::Control, "approval", false),
    (Role::Verifier, Class::Control, "verdict", true),
    (Role::Verifier, Class::Task, "task", false),
    (Role::Verifier, Class::Notify, "notify", false),
    (Role::Verifier, Class::Request, "request", false),
    (Role::Verifier, Class::Reply, "reply", false),
    (Role::Verifier, Class::Control, "steer", false),
    (Role::Verifier, Class::Control, "cancel", false),
    (Role::Verifier, Class::Control, "role", false),
    (Role::LoopSelf, Class::Task, "task", true),
    (Role::LoopSelf, Class::Control, "tick", true),
    (Role::LoopSelf, Class::Notify, "notify", true),
    (Role::LoopSelf, Class::Control, "approval", false),
    (Role::LoopSelf, Class::Control, "verdict", false),
    (Role::LoopSelf, Class::Control, "steer", false),
    (Role::LoopSelf, Class::Control, "role", false),
    (Role::Captain, Class::Task, "task", true),
    (Role::Captain, Class::Control, "approval", true),
    (Role::Captain, Class::Control, "verdict", true),
    (Role::Captain, Class::Control, "steer", true),
    (Role::Captain, Class::Control, "role", true),
    (Role::Captain, Class::Request, "request", true),
    (Role::Captain, Class::Notify, "notify", true),
];

/// `done` cases for [`Authority::deviations`].
pub const SPEC_DONE: &[(Role, bool)] = &[
    (Role::Captain, true),
    (Role::Supervisor, true),
    (Role::Crew, true),
    (Role::LoopSelf, true),
    (Role::Peer, false),
    (Role::Verifier, false),
];

fn unauthorized(
    role: Option<Role>,
    class: Class,
    contract: impl Into<String>,
    expected: Vec<String>,
) -> Reject {
    Reject::Unauthorized {
        role,
        class,
        contract: contract.into(),
        expected,
    }
}

impl Authority {
    /// The table the store enforces.
    pub const fn standard() -> Self {
        Self {
            grants: STANDARD,
            done: STANDARD_DONE,
        }
    }

    /// A custom table (tests and negative controls).
    pub const fn new(grants: &'static [Grant], done: &'static [Role]) -> Self {
        Self { grants, done }
    }

    /// May `role` send `class`/`contract` (a contract name, no major)?
    pub fn allows(&self, role: Role, class: Class, contract: &str) -> bool {
        self.grants
            .iter()
            .any(|g| g.role == role && g.class == class && g.contract.is_none_or(|c| c == contract))
    }

    /// May `role` run `done`?
    pub fn allows_done(&self, role: Role) -> bool {
        self.done.contains(&role)
    }

    /// What `role` may send, as `class/contract` (`class/*` for any contract
    /// of the class) in table order, then `done` when allowed. This is the
    /// `expected` set a refusal carries.
    pub fn expected(&self, role: Role) -> Vec<String> {
        let mut out: Vec<String> = self
            .grants
            .iter()
            .filter(|g| g.role == role)
            .map(|g| format!("{}/{}", g.class.as_str(), g.contract.unwrap_or("*")))
            .collect();
        if self.allows_done(role) {
            out.push(DONE_VERB.to_string());
        }
        out
    }

    /// Every way this table differs from [`SPEC`] / [`SPEC_DONE`] (empty =
    /// agrees). The negative control: an allow-everything table is not empty.
    pub fn deviations(&self) -> Vec<String> {
        let mut out = Vec::new();
        for &(role, class, contract, want) in SPEC {
            let got = self.allows(role, class, contract);
            if got != want {
                out.push(format!(
                    "{role} {}/{contract}: table says {got}, §6.1 says {want}",
                    class.as_str()
                ));
            }
        }
        for &(role, want) in SPEC_DONE {
            let got = self.allows_done(role);
            if got != want {
                out.push(format!("{role} done: table says {got}, §6.1 says {want}"));
            }
        }
        out
    }

    /// May a sender in `standing` append an envelope of `class`/`contract`?
    ///
    /// An ungoverned context (`governed = false`) allows everything, so
    /// contexts without roles are untouched. The operator is always allowed
    /// (role-fact issuance has its own rule, [`Self::check_grant`]).
    pub fn check_send(
        &self,
        standing: Standing,
        governed: bool,
        class: Class,
        contract: &ContractId,
        relation: Relation,
    ) -> Result<(), Reject> {
        if !governed {
            return Ok(());
        }
        let role = match standing {
            Standing::Operator => return Ok(()),
            Standing::Holds(role) => role,
            Standing::Unroled => {
                return Err(unauthorized(None, class, contract.to_string(), Vec::new()));
            }
        };
        let own_verdict = relation == Relation::OwnTask
            && role == Role::Supervisor
            && class == Class::Control
            && contract.name == VERDICT_CONTRACT;
        if self.allows(role, class, &contract.name) && !own_verdict {
            return Ok(());
        }
        let attempted = format!("{}/{}", class.as_str(), contract.name);
        let expected = self
            .expected(role)
            .into_iter()
            .filter(|e| *e != attempted)
            .collect();
        Err(unauthorized(
            Some(role),
            class,
            contract.to_string(),
            expected,
        ))
    }

    /// May a reader in `standing` run `done` on a task of a context?
    pub fn check_done(&self, standing: Standing, governed: bool) -> Result<(), Reject> {
        if !governed {
            return Ok(());
        }
        let role = match standing {
            Standing::Operator => return Ok(()),
            Standing::Holds(role) => role,
            Standing::Unroled => {
                return Err(unauthorized(None, Class::Task, DONE_VERB, Vec::new()));
            }
        };
        if self.allows_done(role) {
            return Ok(());
        }
        let expected = self
            .expected(role)
            .into_iter()
            .filter(|e| e != DONE_VERB)
            .collect();
        Err(unauthorized(Some(role), Class::Task, DONE_VERB, expected))
    }

    /// May `issuer` append `role{subject, new}` when the subject currently
    /// holds `current`? Ordered by rank:
    ///
    /// * A context with no role fact is started only by the operator, and the
    ///   first fact is `captain` (the bootstrap).
    /// * The operator may do anything (it outranks every agent).
    /// * A captain may grant any role but `captain`, and may not touch a
    ///   captain.
    /// * A supervisor may grant the leaf roles (`crew`, `peer`, `loop_self`,
    ///   `verifier`) to subjects that are not themselves a captain or
    ///   supervisor.
    /// * Nobody else may issue a role fact.
    pub const fn may_grant(
        issuer: Standing,
        current: Option<Role>,
        new: Role,
        context_has_roles: bool,
    ) -> bool {
        if !context_has_roles {
            return matches!(issuer, Standing::Operator) && matches!(new, Role::Captain);
        }
        match issuer {
            Standing::Operator => true,
            Standing::Holds(Role::Captain) => {
                !matches!(new, Role::Captain) && !matches!(current, Some(Role::Captain))
            }
            Standing::Holds(Role::Supervisor) => {
                new.is_leaf()
                    && match current {
                        None => true,
                        Some(c) => c.is_leaf(),
                    }
            }
            Standing::Holds(_) | Standing::Unroled => false,
        }
    }

    /// The roles `issuer` could grant to a fresh subject (`role@1:<role>`).
    pub fn grantable(issuer: Standing, context_has_roles: bool) -> Vec<String> {
        Role::ALL
            .into_iter()
            .filter(|r| Self::may_grant(issuer, None, *r, context_has_roles))
            .map(|r| format!("{ROLE_CONTRACT}@1:{r}"))
            .collect()
    }

    /// [`Self::may_grant`] as a typed refusal. `expected` names the roles the
    /// issuer could grant (`role@1:<role>`), or `operator` when it can grant
    /// none.
    pub fn check_grant(
        issuer: Standing,
        current: Option<Role>,
        new: Role,
        context_has_roles: bool,
    ) -> Result<(), Reject> {
        if Self::may_grant(issuer, current, new, context_has_roles) {
            return Ok(());
        }
        let mut expected = Self::grantable(issuer, context_has_roles);
        if expected.is_empty() {
            expected.push("operator".to_string());
        }
        Err(unauthorized(
            issuer.acts_as(),
            Class::Control,
            format!("{ROLE_CONTRACT}@1"),
            expected,
        ))
    }

    /// May `issuer` append a `context@1` fact? The operator, or a captain or
    /// supervisor of the context. Anyone else, no. The first fact has no
    /// captain yet, so only the operator can sign it (Phase 4.2).
    pub const fn may_set_context(issuer: Standing) -> bool {
        matches!(
            issuer,
            Standing::Operator | Standing::Holds(Role::Captain | Role::Supervisor)
        )
    }

    /// May `issuer` append a `lead@1` fact? The first fact of a context is
    /// operator-signed. Once one exists, the operator or the captain may
    /// raise it. A supervisor's control grant does not include `lead`.
    pub const fn may_set_lead(issuer: Standing, has_lead: bool) -> bool {
        if !has_lead {
            return matches!(issuer, Standing::Operator);
        }
        matches!(issuer, Standing::Operator | Standing::Holds(Role::Captain))
    }

    /// [`Self::may_set_lead`] as a typed refusal.
    pub fn check_set_lead(issuer: Standing, has_lead: bool) -> Result<(), Reject> {
        if Self::may_set_lead(issuer, has_lead) {
            return Ok(());
        }
        let expected = if has_lead {
            vec!["operator".into(), "captain".into()]
        } else {
            vec!["operator".into()]
        };
        Err(unauthorized(
            issuer.acts_as(),
            Class::Control,
            "lead@1",
            expected,
        ))
    }

    /// [`Self::may_set_context`] as a typed refusal. `expected` names who can.
    pub fn check_set_context(issuer: Standing) -> Result<(), Reject> {
        if Self::may_set_context(issuer) {
            return Ok(());
        }
        Err(unauthorized(
            issuer.acts_as(),
            Class::Control,
            format!("{CONTEXT_CONTRACT}@1"),
            vec!["operator".into(), "captain".into(), "supervisor".into()],
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(name: &str) -> ContractId {
        ContractId::new(name, 1)
    }

    fn send(role: Role, class: Class, name: &str) -> Result<(), Reject> {
        Authority::standard().check_send(
            Standing::Holds(role),
            true,
            class,
            &c(name),
            Relation::Other,
        )
    }

    /// An allow-everything table: the stub the negative control runs.
    static ALLOW_ALL: [Grant; 36] = {
        let mut out = [any(Role::Captain, Class::Notify); 36];
        let mut i = 0;
        while i < 6 {
            let mut j = 0;
            while j < 6 {
                out[i * 6 + j] = any(Role::ALL[i], Class::ALL[j]);
                j += 1;
            }
            i += 1;
        }
        out
    };

    #[test]
    fn the_standard_table_is_redesign_6_1() {
        assert_eq!(Authority::standard().deviations(), Vec::<String>::new());
    }

    #[test]
    fn a_stub_that_allows_everything_deviates() {
        let stub = Authority::new(&ALLOW_ALL, &Role::ALL);
        let d = stub.deviations();
        assert!(
            d.len() > 20,
            "an allow-all table must fail the spec on every denial: {d:?}"
        );
        assert!(d.iter().any(|l| l.contains("crew control/verdict")));
        assert!(d.iter().any(|l| l.contains("peer task/task")));
        assert!(d.iter().any(|l| l.contains("verifier done")));
    }

    /// A refusal carries the role's WHOLE allow-list, not the slice for the
    /// attempted class, so the same set answers a Task refusal and a Control
    /// refusal. Crew holds exactly one control verb, `escalate@1`, the upward
    /// handoff, which is why `control/escalate` sits beside the three
    /// any-class verbs. Phase 4a added that grant; this test has to move with
    /// it or it reds the suite against a promise the table keeps.
    #[test]
    fn crew_cannot_task_verdict_or_approve() {
        let allow_list: Vec<String> = [
            "notify/*",
            "request/*",
            "reply/*",
            "control/escalate",
            "done",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        for (name, class) in [
            ("task", Class::Task),
            ("verdict", Class::Control),
            ("approval", Class::Control),
        ] {
            let r = send(Role::Crew, class, name).unwrap_err();
            let Reject::Unauthorized {
                role,
                class: got,
                contract,
                expected,
            } = r
            else {
                panic!("not Unauthorized");
            };
            assert_eq!(role, Some(Role::Crew));
            assert_eq!(got, class);
            assert_eq!(contract, format!("{name}@1"));
            assert_eq!(expected, allow_list);
            // Same invariant the own-task verdict test pins: a refusal never
            // offers back the verb it just refused.
            let leaked = match class {
                Class::Task => expected.contains(&"task/*".to_string()),
                _ => expected.iter().any(|e| e == &format!("control/{name}")),
            };
            assert!(!leaked, "the attempted verb is offered back: {expected:?}");
        }
        // The other half of the contract: the one control verb crew DOES
        // hold. Without it a crew cannot report upward, and the captain push
        // (escalate mail) rides this grant.
        assert!(send(Role::Crew, Class::Control, "escalate").is_ok());
    }

    #[test]
    fn a_peer_cannot_task_or_control() {
        assert!(send(Role::Peer, Class::Task, "task").is_err());
        for name in ["steer", "cancel", "verdict", "approval", "role"] {
            assert!(send(Role::Peer, Class::Control, name).is_err(), "{name}");
        }
        for class in [Class::Request, Class::Reply, Class::Notify] {
            assert!(send(Role::Peer, class, "x").is_ok(), "{class:?}");
        }
    }

    #[test]
    fn a_supervisor_tasks_steers_and_cancels_but_never_approves() {
        assert!(send(Role::Supervisor, Class::Task, "task").is_ok());
        assert!(send(Role::Supervisor, Class::Control, "steer").is_ok());
        assert!(send(Role::Supervisor, Class::Control, "cancel").is_ok());
        assert!(send(Role::Supervisor, Class::Control, "approval").is_err());
    }

    #[test]
    fn a_supervisor_cannot_verdict_its_own_task_but_can_verdict_anothers() {
        let a = Authority::standard();
        let verdict = c(VERDICT_CONTRACT);
        let own = a.check_send(
            Standing::Holds(Role::Supervisor),
            true,
            Class::Control,
            &verdict,
            Relation::OwnTask,
        );
        let Err(Reject::Unauthorized { role, expected, .. }) = own else {
            panic!("own-task verdict must be refused: {own:?}");
        };
        assert_eq!(role, Some(Role::Supervisor));
        assert!(
            !expected.contains(&"control/verdict".to_string()),
            "the attempted verb is not offered back: {expected:?}"
        );
        assert!(expected.contains(&"control/steer".to_string()));
        assert!(
            a.check_send(
                Standing::Holds(Role::Supervisor),
                true,
                Class::Control,
                &verdict,
                Relation::Other,
            )
            .is_ok()
        );
    }

    #[test]
    fn a_verifier_may_only_verdict_and_never_done() {
        assert!(send(Role::Verifier, Class::Control, "verdict").is_ok());
        for class in [Class::Task, Class::Notify, Class::Request, Class::Reply] {
            assert!(send(Role::Verifier, class, "x").is_err(), "{class:?}");
        }
        let d = Authority::standard().check_done(Standing::Holds(Role::Verifier), true);
        let Err(Reject::Unauthorized {
            contract, expected, ..
        }) = d
        else {
            panic!("verifier done must be refused: {d:?}");
        };
        assert_eq!(contract, "done");
        assert_eq!(expected, ["control/verdict"]);
        assert!(
            Authority::standard()
                .check_done(Standing::Holds(Role::Crew), true)
                .is_ok()
        );
    }

    #[test]
    fn loop_self_tasks_and_ticks_but_cannot_approve() {
        assert!(send(Role::LoopSelf, Class::Task, "task").is_ok());
        assert!(send(Role::LoopSelf, Class::Control, "tick").is_ok());
        assert!(send(Role::LoopSelf, Class::Control, "approval").is_err());
    }

    #[test]
    fn the_captain_may_send_any_class() {
        for class in Class::ALL {
            assert!(send(Role::Captain, class, "anything").is_ok(), "{class:?}");
        }
    }

    #[test]
    fn an_ungoverned_context_allows_everything_and_a_governed_one_needs_a_role() {
        let a = Authority::standard();
        let task = c("task");
        assert!(
            a.check_send(
                Standing::Unroled,
                false,
                Class::Task,
                &task,
                Relation::Other
            )
            .is_ok()
        );
        let r = a
            .check_send(Standing::Unroled, true, Class::Task, &task, Relation::Other)
            .unwrap_err();
        assert_eq!(
            r,
            Reject::Unauthorized {
                role: None,
                class: Class::Task,
                contract: "task@1".into(),
                expected: vec![],
            }
        );
        assert!(
            a.check_send(
                Standing::Operator,
                true,
                Class::Control,
                &c("approval"),
                Relation::Other
            )
            .is_ok(),
            "the operator outranks every role"
        );
    }

    #[test]
    fn who_may_issue_which_role() {
        use Role::*;
        use Standing::*;
        let g = Authority::may_grant;
        // Bootstrap: only the operator, only captain.
        assert!(g(Operator, None, Captain, false));
        assert!(!g(Operator, None, Crew, false));
        assert!(!g(Holds(Captain), None, Captain, false));
        assert!(!g(Unroled, None, Captain, false));
        // Operator outranks everyone.
        assert!(g(Operator, Some(Captain), Peer, true));
        assert!(g(Operator, None, Supervisor, true));
        // Captain: not captain, not over a captain.
        assert!(g(Holds(Captain), None, Supervisor, true));
        assert!(g(Holds(Captain), Some(Crew), Verifier, true));
        assert!(!g(Holds(Captain), None, Captain, true));
        assert!(!g(Holds(Captain), Some(Captain), Peer, true));
        // Supervisor: leaf roles over non-supervisors.
        assert!(g(Holds(Supervisor), None, Crew, true));
        assert!(g(Holds(Supervisor), Some(Peer), Verifier, true));
        assert!(!g(Holds(Supervisor), None, Supervisor, true));
        assert!(!g(Holds(Supervisor), None, Captain, true));
        assert!(!g(Holds(Supervisor), Some(Supervisor), Crew, true));
        assert!(!g(Holds(Supervisor), Some(Captain), Crew, true));
        // Everyone else: nothing.
        for r in [Crew, Peer, LoopSelf, Verifier] {
            for n in Role::ALL {
                assert!(!g(Holds(r), None, n, true), "{r} granting {n}");
            }
        }
        assert!(!g(Unroled, None, Crew, true));
    }

    #[test]
    fn a_refused_grant_names_what_the_issuer_could_grant() {
        let r =
            Authority::check_grant(Standing::Holds(Role::Supervisor), None, Role::Captain, true)
                .unwrap_err();
        let Reject::Unauthorized {
            role,
            class,
            contract,
            expected,
        } = r
        else {
            panic!("not Unauthorized");
        };
        assert_eq!(role, Some(Role::Supervisor));
        assert_eq!(class, Class::Control);
        assert_eq!(contract, "role@1");
        assert_eq!(
            expected,
            [
                "role@1:crew",
                "role@1:peer",
                "role@1:loop_self",
                "role@1:verifier"
            ]
        );
        let none = Authority::check_grant(Standing::Holds(Role::Crew), None, Role::Peer, true)
            .unwrap_err();
        assert!(matches!(
            none,
            Reject::Unauthorized { expected, .. } if expected == ["operator"]
        ));
        let bootstrap =
            Authority::check_grant(Standing::Operator, None, Role::Crew, false).unwrap_err();
        assert!(matches!(
            bootstrap,
            Reject::Unauthorized { expected, .. } if expected == ["role@1:captain"]
        ));
    }

    #[test]
    fn roles_round_trip_through_their_wire_spelling() {
        for r in Role::ALL {
            assert_eq!(Role::parse(r.as_str()), Some(r));
            assert_eq!(
                serde_json::to_value(r).unwrap(),
                serde_json::json!(r.as_str())
            );
        }
        assert_eq!(Role::parse("Captain"), None);
        assert_eq!(Role::parse("operator"), None);
    }
}
