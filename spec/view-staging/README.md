# Materialised view staging and source admission

These two focused companions check independent-clock local publication closure
and source revision admission. They compose through the explicit contract below,
not by claiming the full Cartesian product of all source, topology and view
features has been checked.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `MaterialisedViewReadyClosure` | 5 | 2 | 9 | 12 | 14 | 46,846 |
| `MaterialisedViewSourceAdmission` | 6 | 8 | 15 | 21 | 27 | 22,720 |

## Independent-clock ready closure

The base instance has three two-key batches over a three-key ring. Batch t
writes its own key at HLC 30 and its neighbour at HLC 10. Every batch has the
same maximum HLC, but no serial ordering of the batches produces the source's
per-key winners without an observable intermediate wrong cut. Omitting a
batch's low-clock key relies on its neighbour's high-clock replacement; the
dependency closes transitively around the ring. Batch 2 additionally has an
external eligibility hold, separate from its fully consumed prepare/terminal
log. Even after every captured head is consumed, the other batches must wait
wholly while that replacement is outside the eligible flush set. Fair `Release`
removes the hold; only then can the ring publish. The closure-only negative
control preserves the horizon gate, proving these are distinct obligations.

The fixed reachable prefix has all source decisions committed and installed,
but no consumer has read any prepare or terminal. Each source-key partition has
two prepares followed by two distinct shard terminals. A subscription read
consumes one entry, staging independently by partition. A complete local batch
captures the confirmation head, then waits for both head consumption and its
whole replacement closure before one atomic publication of the per-key maxima.
Checkpoint writes cannot pass unresolved entries. Crashes discard read progress,
staging and the volatile confirmation horizon; the durable publication and its
idempotent completion survive even if no checkpoint followed it.

The base chooses either a current-state or an accumulative registration at
initialisation, so history gates are not vacuous controls on a current-only
base. History publishes every complete two-key batch's revision identities
atomically, not only its current winners. The `History` variant isolates that
branch. The `TwoConsumers` variant uses a two-batch/two-key cycle and two
independently progressing current-state consumers. Each can observe a different
lag and finish at a different time; no same-time cross-view flip is asserted.
Public snapshot actions cannot mix keys across a local closure or split a
history batch's revision pair.

## Coherent source admission

The source-admission base likewise covers current-state and history
registrations. Its two prepared rows have HLCs 20/40. A newer live row at HLC 50
has identical bytes to the first prepare; the second key has an empty live
prepare and an empty-byte tombstone. An adversarial source alternative holds
the tombstone at the prepare's HLC until a fair newer source revision arrives.
That alternative discriminates ignoring kind even when bytes and clock match;
it is not a claim that well-formed source writes reuse a revision identity.

Commit decision, source installation, terminal append and per-partition terminal
consumption are separate steps. A terminal can arrive before installation is
readable. A confirmation samples whole row tuples: timestamp, bytes and
tombstone kind. Matching bytes are not revision identity, and a source sample
does not create a synthetic projection mutation. A mismatching newer source
row arms a post-observation head. Consuming that head is necessary but not
sufficient: an ordinary entry can have been dropped by the upstream admission
gate. The batch stays wholly deferred until an actually applied replacement
supports every omission. A later repair publishes the real replacement before
the batch can finish and advance its checkpoint.

Absent and older source rows remain pending even after the terminal horizon is
fully consumed. A same-version nonmatching row also stays pending; only exact
identity or a strictly newer revision can discharge admission. `AdmissionComplete`
and its lagging-row mutant discriminate retiring such a pending batch. Both the
base and `PendingSource` include absent and older rows, so the paired control
exercises lagging installation rather than assuming it away.

`PendingOverlay` isolates an undrained B prepare over an already committed and
materialised A prefix. B is independently in flight or committed, with a newer
HLC 50 and the same bytes as A's HLC 20 row. A coherent materialised row is not
necessarily the logically visible row: one bounded leaf capture includes pending
metadata and its resolved decision witness. Confirmation uses the committed
pending tuple, while in-flight metadata falls through to the materialised row.
B can commit or materialise independently of A's terminal delivery. Retiring B's
pending metadata must first install its tuple, preserving logical visibility.
The base includes this overlay branch as well as the earlier lag/kind branches.
Clocks remain independent of WAL offsets 1..3.

`DecisionEntitled` and the raw-only confirmation mutant discriminate publishing
older A after observing B's committed metadata. `LogicalVisibilityMonotone`
and its metadata-drop mutant discriminate revealing old materialised state on
retirement. A separate lost-decision mutant violates eventual convergence.
These controls preserve the existing tuple, horizon and replacement gates.
Captured in-flight evidence does not promise freshness against a later B commit.
Only key1 is contended by this overlay; no source-wide atomic snapshot is assumed.
Abort, indeterminate/unavailable resolution, expiry and terminal-orphan handling
remain source-seam obligations outside this two-decision-state abstraction.
History retains original A's revisions; complete B-history processing is not
expanded. B's actual replacement publication is a ReadyClosure rely-guarantee,
not a synthetic mutation copied from its sampled metadata.

`PendingSource` isolates decision/terminal-before-source-installation.
`History` isolates committed revision retention despite newer current state.
`CrossTree` requires readiness from both expected participants rather than
one local registration. The external participant is an abstract readiness
certificate; the model does not implement its drain, the joint-write engine,
coordinator timeout/degrade or simultaneous reader visibility across views.
Actual maintainer admission and expected-peer controls now establish these bounded caller seams; the joint-publication lifecycle is a separate companion, not a shared cross-view snapshot.

The consumer's sample/publish turn excludes its own ordinary-write processing
between confirmation and publication. It does not freeze the source. In this
bounded source, sampling a vector is equivalent to sampling each row coherently:
one conflicting tuple can progress while the other is stable, or installation
only advances the two missing rows. No source-wide snapshot isolation claim
is inferred from this representation.

## Composition and limits

ReadyClosure relies on SourceAdmission's guarantee that an omitted revision
has a *known eligible replacement*, a coherent source identity, and sufficient
terminal/head evidence. Its fixed installed source prefix is that guarantee's
instantiated precondition, not proof that local completion establishes it.
SourceAdmission relies on the local closure's single atomic slice and per-key
clock fold when ready overlapping replacements form a dependency cycle.
Neither same bytes, a scalar maximum batch clock, nor one shard/participant
terminal discharges either side of this rely-guarantee contract.

Physical WAL rebinding, dual candidate holds, GC, retained replay fences and
multi-partition orphan rebuild are checked separately by Tail, Rebind, Fence
and Horizon. However, these models keep one physical log identity fixed. Durable
completion/idempotency must be scoped to that identity: the resolved-Guid reuse
flaw #4895 is not represented by these models or by the current single-transaction
Rebind model. The separate ResolvedIdentity companion now checks two physical
logs reusing one finite transaction ID, actual-tail cache ownership and eviction
queue scope. It does not multiply those dimensions into admission/ready closure;
no reset of a cache absent from a model can prove that property.

Aggregation membership/numeric-intent composition is the
Aggregation model's; these models do not equate revision history with a
current-state numeric fold. Fair source completion/correction and maintenance
follow finitely many crashes. The admission read/repair uses strong fairness
because its eligibility recurs between deferred confirmation turns; no
convergence is promised under permanent starvation, infinite faults or an
uncorrected contradictory source tuple.

## Evidence and acceptance

Checks are exhaustive, not simulations. Every checked property has a targeted
mutant and every behavioural action has an anchored perturbation. Serial-batch
ordering, split publication, incomplete transitive closure, equal-byte identity,
tombstone-kind identity and treating dropped ordinary progress as applied
replacement each have a separate negative control.

The refinement notes map accepted bounded production seams to exact paired
controls: deterministic transitive local closure, post-observation head
consumption, whole-batch logical source admission, expected-participant closure,
durable holdback and finite lost-response replay. Actual caller mutations fail
at those boundaries, not merely a collaborator helper or final value. No
source-wide atomic capture, arbitrary cross-view snapshot or unconditional
availability guarantee is inferred from a clean finite model.

## Measured exhaustive instances

| Instance | Distinct states | Raw seconds |
|----------|-----------------|-------------|
| ReadyClosure base | 46,846 | 10.803 |
| ReadyClosure History | 39,651 | 8.713 |
| ReadyClosure TwoConsumers | 31,577 | 10.780 |
| SourceAdmission base | 22,720 | 8.380 |
| SourceAdmission CrossTree | 836 | 3.032 |
| SourceAdmission History | 16,412 | 8.415 |
| SourceAdmission PendingSource | 660 | 1.767 |
| SourceAdmission PendingOverlay | 1,028 | 1.931 |

Actual repository TlcModelCheckTests measurements, not a proxy runner:

| Module | Passed / executed | Item span seconds | Whole command seconds |
|--------|-------------------|-------------------|-----------------------|
| MaterialisedViewReadyClosure | 15 / 15 | 87.575177 | 94.200066 |
| MaterialisedViewSourceAdmission | 26 / 26 | 220.225517 | 239.837819 |

Each module includes its base, every variant and every paired mutation case.
The recorded scopes total 41 passed cases with zero skipped, inconclusive or
failed cases. The combined raw exhaustive census has 74 JVM runs: eight
base/variant runs and 33 distinct control/mutant pairs. Single-property experiments
retain TypeOK;
mutants must violate the intended property, not a parser or domain failure.
Mutation-specific generated headers keep same-target controls distinct; no
shared-control speedup is claimed.

Measurements used Java 17 / TLC 2.19, one JVM slot, two visible CPUs, two TLC
workers, ParallelGC and a maximum heap of 1,800 MiB (TLC reported 1,600 MB usable
heap). Peak live heap was not measured. Whole command includes no-build test
startup/discovery overhead; item span is earliest case start to latest case end.
The admission harness overlapped the non-TLC structural scope on the shared host;
these timings are local observations, not isolated performance comparisons or
predicted CI performance.

The overlay-owned non-TLC structural scope passed 49/49; six separate refinement
census/declaration checks passed, including the 27-row behaviour denominator.
Those original extension runs preceded parent integration and did not certify
root index enrollment or caller acceptance. The composed parent now passes the
full 384-case refinement acceptance/resolver scope without waived verdicts,
alongside 92 caller/physical and 35 real runtime cases. Their exact negative
controls and remaining relies are recorded in the refinement notes. Exhaustive
TLC runs remain in the dedicated CI shards rather than a repeated local sweep.
