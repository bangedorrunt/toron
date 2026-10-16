<!-- governed-by: ADR-0041 -->
# Comms Phase 4.1: roles and authority at append — API surface

Status: landed with the Phase 4.1 PR. Redesign §6.1; build brief `phase4-topologies.md` item 4.1.
Consumer: flywheel (Phase 4a supervisor, 4.2 topology). Everything below is `toron-core`,
behind the comms flag (`TORON_COMMS_INBOX` / `[comms] inbox_log`). Flag off: nothing here runs.

## Semantics in six lines

1. A **role** is a log fact: a `role@1` Control envelope `role{npub, role, context, gen}`
   (`context` = the envelope's `context`; `gen` fences per `(context, npub)`, strictly increasing).
   `inbox_role` (migration `0024`) is its projection, written in the same transaction as the append.
2. A context with **no role fact is ungoverned**: no check applies, behaviour is exactly today's.
   The first role fact **governs** it; from then on a sender that is neither the operator nor a role
   holder is refused (`role: null`).
3. **Bootstrap:** the first fact of a context must be `role{captain}` appended by the **operator key**
   (`from == operator npub`). Nobody else can start a context; the operator can start it with nothing else.
4. **Who may issue which role** (`Authority::may_grant`): operator, anything. Captain, any role but
   `captain`, and never over a captain. Supervisor, the leaf roles (`crew|peer|loop_self|verifier`)
   over subjects that are not captain/supervisor. Everyone else, nothing. A re-issue needs a higher `gen`
   (`Reject::StaleGen{sent, current}`). There is no revoke fact: re-grant a lower role at a higher `gen`.
5. **Enforcement is inside the append transaction**, after the epoch fence and the replay answer, before
   any write (`InboxStore::append_checked`, which both `append_envelope` and the kernel `append_json` /
   `ToronLog` path share). A refusal writes nothing and consumes no producer sequence number. A replay of
   an already-appended triple stays idempotent even if the sender's role changed since.
6. The **operator acts with captain authority** in every context and is exempt from the table
   (it still follows the grant bootstrap). "Operator" = the npub handed to `InboxStore::with_operator`;
   `Mailbox::inbox_store_with` does this from the loaded operator key.

## The table (`authority::STANDARD`, redesign §6.1)

`expected` strings are `class/contract-name` (`class/*` = any contract of the class), then `done`.

| role | may send | refused |
|---|---|---|
| captain | `notify/* request/* reply/* task/* control/* receipt/*`, `done` | – |
| supervisor | `notify/* request/* reply/* task/* receipt/*`, `control/{steer,cancel,goal,escalate,verdict,role}`, `done` | `control/approval`; `verdict` on a task it sent (`Relation::OwnTask`) |
| crew | `notify/* request/* reply/*`, `done` | task, any control (verdict, approval, steer, cancel, role) |
| peer | `notify/* request/* reply/*` | task, any control, `done` |
| loop_self | `task/* notify/* receipt/*`, `control/tick`, `done` | approval, verdict, steer, role |
| verifier | `control/verdict` | everything else; `done` |

Default-deny: a pair not in the table is refused. `steer`, `cancel`, `approval`, `goal`, `escalate`,
`tick` are named in the table but are **not contracts yet** (an envelope naming one is
`Reject::UnsupportedContract` until the phase that adds it). `Authority::deviations()` checks the table
against an independent case list (`authority::SPEC`); an allow-everything table fails it.

## Public Rust API

```rust
// toron_core::authority   (pure: std + serde, no clock, no store; in tests/fold_purity.rs)
pub enum Role { Captain, Supervisor, Crew, Peer, LoopSelf, Verifier }       // serde snake_case:
//   "captain" | "supervisor" | "crew" | "peer" | "loop_self" | "verifier"
impl Role { pub const ALL: [Role; 6]; pub const fn as_str(self) -> &'static str;
            pub fn parse(&str) -> Option<Role>; pub const fn is_leaf(self) -> bool }
pub enum Standing { Operator, Holds(Role), Unroled }   // acts_as() -> Option<Role> (operator => Captain)
pub enum Relation { Other, OwnTask }                    // OwnTask: sender sent a `task` envelope for env.task
pub struct Grant { pub role: Role, pub class: Class, pub contract: Option<&'static str> }
pub const STANDARD: &[Grant];   pub const STANDARD_DONE: &[Role];
pub const SPEC: &[(Role, Class, &str, bool)];   pub const SPEC_DONE: &[(Role, bool)];
#[derive(Clone, Copy, Debug)] pub struct Authority { /* grants, done */ }  // Default = standard()
impl Authority {
    pub const fn standard() -> Self;
    pub const fn new(grants: &'static [Grant], done: &'static [Role]) -> Self;     // tests / negative controls
    pub fn allows(&self, role: Role, class: Class, contract_name: &str) -> bool;   // name, no major
    pub fn allows_done(&self, role: Role) -> bool;
    pub fn expected(&self, role: Role) -> Vec<String>;
    pub fn deviations(&self) -> Vec<String>;                                        // vs SPEC; empty = agrees
    pub fn check_send(&self, standing: Standing, governed: bool, class: Class,
                      contract: &ContractId, relation: Relation) -> Result<(), Reject>;
    pub fn check_done(&self, standing: Standing, governed: bool) -> Result<(), Reject>;
    pub const fn may_grant(issuer: Standing, current: Option<Role>, new: Role,
                           context_has_roles: bool) -> bool;                        // associated fn
    pub fn grantable(issuer: Standing, context_has_roles: bool) -> Vec<String>;     // "role@1:<role>"
    pub fn check_grant(issuer: Standing, current: Option<Role>, new: Role,
                       context_has_roles: bool) -> Result<(), Reject>;
}

// toron_core::delivery  (closed enum, internally tagged "reject"; struct variant on purpose)
Reject::Unauthorized { role: Option<Role>, class: Class, contract: String, expected: Vec<String> }
//   wire: {"reject":"unauthorized","role":"crew"|null,"class":"task","contract":"task@1","expected":[...]}
//   role=None   => governed context, sender holds no role (expected is empty)
//   contract    => "name@major"; the verb name "done" for `toron done`; "role@1" for a refused role fact
//   expected    => what this role may send instead; for a refused role fact, the roles the issuer
//                  could grant ("role@1:crew", ...), or ["operator"] when it could grant none
// plus, for a role fact whose gen is not above the one on record: Reject::StaleGen { sent, current }

// toron_core::contracts
pub const KNOWN  // gains ("role", 1, Class::Control) and ("verdict", 1, Class::Control)
enum Body { .., Role(role_v1::RoleV1), Verdict(verdict_v1::VerdictV1) }
// contracts::role_v1
pub struct RoleV1 { pub npub: String, pub role: Role, #[serde(rename = "gen")] pub generation: u32 } // deny_unknown_fields
impl RoleV1 { pub fn into_envelope(self, msg: MsgId, context: ContextId) -> Envelope }  // from=msg.producer,
//   to=Npub(self.npub), class=Control, contract=role@1, task=None. validate(): to == npub, npub is hex.
// contracts::verdict_v1 (PROVISIONAL shape, 4.3 owns it; a verdict is a log fact only until 4.3)
pub struct VerdictV1 { pub pass: bool, #[serde(default)] pub evidence_refs: Vec<String> } // needs env.task

// toron_core::inbox::InboxStore
pub fn with_operator(self, npub: impl Into<String>) -> Self;       // 64-hex; without it no context can start
pub fn with_authority(self, a: Authority) -> Self;                  // tests only
pub fn operator(&self) -> Option<&str>;
pub async fn append_envelope(&self, env: &Envelope) -> Result<Appended, InboxError>;
//   Err(InboxError::Reject(Reject::Unauthorized{..} | Reject::StaleGen{..}))
pub async fn authorize(&self, env: &Envelope) -> Result<(), InboxError>;   // dry run: reads roles, writes nothing
pub async fn standing(&self, context: &str, npub: &str) -> Result<Standing, sqlx::Error>;
pub async fn role_of(&self, context: &str, npub: &str) -> Result<Option<RoleFact>, sqlx::Error>;
pub async fn roles(&self, context: &str) -> Result<Vec<RoleFact>, sqlx::Error>;  // ordered by npub; empty = ungoverned
pub struct RoleFact { pub context: String, pub npub: String, pub role: Role,
                      #[serde(rename = "gen")] pub generation: u32, pub msg_key: String, pub issuer: String }
// toron_core::mailbox::Mailbox::inbox_store_with(..) attaches the operator npub itself.
```

## What flywheel must know

* **Typed `Unauthorized` is only on the typed entries.** `append_envelope` and `authorize` return
  `InboxError::Reject(Reject::Unauthorized{..})`. The kernel entries (`InboxStore::append_json`, `ToronLog`,
  therefore `outbox::send_and_recover` and `toron inbox send`) cannot carry it: the vendored `AppendError`
  is closed, so a refused send surfaces as `SendState::Refused("not a log envelope: role crew may not send …")`
  (a string; `toron inbox send` still exits 0 — this predates 4.1 for every refusal). A sender that wants the
  typed reject and the expected set calls `InboxStore::authorize(&env)` before sending.
* Open the store through `Mailbox::inbox_store*` (it knows the operator) or add `.with_operator(npub)`.
  A store opened with `InboxStore::new` alone has no operator: it enforces roles but nobody on it can start
  a context, and the operator's own sends in a governed context read as unroled.
* Spawn order for a governed context: operator-signed `role{captain}`, then the captain (or a supervisor,
  for leaf roles) appends the rest. Seq/epoch rules are the ordinary L16 ones: a refused role fact consumes
  no seq.
* `Role`/`Standing` are plain data; to project a topology (4.2) read `roles(context)` and hand the
  viewer's `Standing` to the pure projection.
* Refusals the flywheel should match: `Reject::Unauthorized` (new), `Reject::StaleGen` (role gen).
