# Materialised-view joint repair

This companion models one original rendezvous followed by a mandatory fresh
joint repair and durable participant acknowledgement closure. It complements
the local ReadyClosure and SourceAdmission models; it does not replace their
source-admission or replacement-closure obligations.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `MaterialisedViewJointRepair` | 16 | 3 | 21 | 25 | 38 | 11,006 |

## Protocol and observables

Two original participants register before one original decision and joint flip.
Only an undecided original rendezvous may timeout into degradation. A durable
decision cannot abandon joint publication after a lost response.

Every original participant then submits a fresh coherent source projection,
including an empty contribution. The first submission freezes its payload and
physical view target. Fresh readiness cannot be replaced by old readiness, a
later retry, or rows copied from the existing projection. One independent repair
operation publishes all captured slices jointly. Failed completion persistence
cannot report success; reactivation retains the first capture. A retry after
repair completion must not replay old captured rows over a newer ordinary write.

The model exposes the joint publication phase and actual row values in one
conceptual state cut. `Observe` is a proof witness, not a supported shared
multi-view snapshot cursor. Independent V1/V2 opens can straddle a joint commit.
Runtime refinement must hold/capture the actual joint write, rather than infer
simultaneity from sequential reads.
An ordinary/local publication fence holds until repair completes. Checkpoint
retirement follows completion; acknowledgements follow durable retirement;
retention cleanup follows every original participant's acknowledgement.
Physical rebind is prohibited until that participant's durable retirement.
This protocol adds no multi-source join-view or view-DAG guarantee.

## Rely-guarantee boundary and liveness

`InitCurrent` starts after the bounded participants' durable source/view identity
holds are established. `InitLegacy` instead imports a released, completed and
already-retired original operation without manufacturing fresh repair evidence.
Creating those holds before registration, and ordinary
publication before a hold exists, are outside this companion; the preventative
ordinary-versus-rendezvous runtime detectors cover that earlier ordering seam.

`Capture` relies on the maintainer consuming sampled post-observation WAL
horizons and closing all transitive replacement dependencies before submitting
the complete current projection. The scalar source version stands for an
admitted coherent HLC/kind/bytes tuple, including rekey retractions and numeric
membership rows. This model does not prove that source-to-projection mapping.

`Retire` relies on one successful checkpoint write removing the hold and adding
the pending acknowledgement only after replayed ordinary work covers the held
retirement frontier. `Crash` preserves that durable state. Those rely
obligations are not consequences of coordinator API success.
The ordinary action accepts coherent current source data; the caller's durable
repair-source HLC witnesses must separately reject older deferred WAL replay.
The bounded row witness is one projected cell per participant, not a proof of
complete delete/rekey/numeric-membership payload encoding.

The base explores both current work and historical completed legacy records.
Legacy original completion expires automatically under its shipped contract;
`LegacyNeverCertified` prohibits inventing fresh captures, repair completion or
acknowledgements from that original `Applied` flag. This is a versioned contract
boundary, not a repair-success shortcut or manual migration requirement.

Automatic patch onboarding pauses new protocol RPCs/holds while any non-dead
member has missing or old capability, then resumes after monotonic replacement.
The real coordinator admission and manifest-cache detectors cover this earlier
gate, outside `InitCurrent`; finite TLC does not establish membership atomicity.
Legacy in-flight response/write recovery has separate coordinator controls.
Arbitrary old-binary rejoin/downgrade after current admission is not a supported
rollout or an assumed-safe placement transition.

The model assumes eventual peer recovery, fair maintenance and successful
persistence after a bounded number of faults. It deliberately has no degraded
repair transition. An unavailable original participant pins checkpoint and
physical-identity holds indefinitely; fair recovery is required for convergence.
No liveness claim covers a permanently unavailable peer.

Fair `Capture` also relies on the complete replacement closure fitting the
configured staging capacity, or that capacity eventually being restored.
Permanent capacity exhaustion is not waived by fairness: callers must fail
closed retaining the obligation. Once a capture is frozen, waiting for a peer
does not entitle the caller to keep accumulating WAL staging. Resource sizing
and that polling discipline are caller obligations outside this bounded model.

## Runtime refinement status

The source-owner caller detector set is integrated by #4876 caller commit
`41fa9e00` and automatic upgrade commit `77c6c127`; the physical topology
correction is #4905 commit `695d09a0`. The committed source handoff reports
105/105 core and 25/25 runtime cases green with no skips. Actual-source mutation
controls fail for ordinary publication bypass (2 cases), acknowledgement before
checkpoint (2), peer-wait staging growth (1), stale source-version replay (the
missing-metadata case fails while its readable-metadata control passes), and
held physical routing (3). The alias-rebuild point control also fails when its
physical topology resolver is bypassed. See the refinement table for the exact
detectors and their limits; these results do not promote unexercised source
closure or arbitrary topology transitions to proven properties.

Automatic upgrade drain admission is tested before coordinator protocol RPCs,
with automatic resumption and retained current repair debt. Separate #4902
correction `1c67d925` also gates public decommission, rebuild and reconcile with
pending joint ownership/acknowledgement before RPC or state/generation effects.
Its three pre-fix failures are restored green; the parent composed upgrade/ack
scope passes 8/8 and adjacent replay/decommission passes 65/65. Reopened
Decommission delivers the retained acknowledgement before an injected response
fault, not a claimed complete rebuild/reconcile-on-reopen. Liveness remains
conditional on peer recovery, fair maintenance and
available staging capacity. Paired-view checks witness stable held publication
boundaries only; independent opens may straddle a valid joint commit.

## Bounds and variants

The base has two participants, coherent versions 1 and 2, independently selected
physical targets 0 and 1, and one bounded fault. `SingleTarget` fixes the initial
physical target to 0 (4,572 distinct states); `TwoFaults` permits two failures
(16,437 distinct states). `Legacy` isolates automatic historical cleanup and
no-fresh-certification (144 distinct states); the base includes both modes so
its legacy mutation controls are not vacuous. Versions describe source
replacements, not WAL offsets. Ordinary writes can follow repair completion
before durable checkpoint retirement, reflecting deferred replay.

The fault actions abstract lost responses, completion-write failure and
reactivation without expanding every await into a separate state. Publication
effects are idempotent. Original and repair operation identities are distinct
and assumed collision-free, as required by the implementation's `mv-xt-` and
`mv-repair-xt-` prefixes.

## Mutation evidence

Every checked invariant/property has a paired exact-target mutation. Every
behavioural `Next` action is perturbed; `Stutter` is explicitly non-behavioural.
Controls run the current base against the mutant's one target plus `TypeOK`;
mutants must report that target, not a parse error or unrelated violation.

Mutations exercise missing peers, discarded durable original intent, undurable
success, stale or mutable fresh capture, split joint publication, wrong physical
targets, premature ordinary publication/checkpoint/ack/cleanup, lost progress,
torn or incorrect conceptual publication-cut witnesses, replay over a newer
ordinary peer, historical completion misclassified as fresh repair, and legacy
TTL mistakenly applied to unclosed current work.

The [refinement note](RefinementJointRepair.md) separates coordinator seam
detectors from pending maintainer/runtime obligations. Finite TLC results do
not promote those pending obligations to accepted production refinement.
