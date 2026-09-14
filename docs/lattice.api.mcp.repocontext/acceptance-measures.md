# Acceptance-measure register

The measures a scored deployment run of epic #2368 is graded against, one row
per measure, with the arithmetic behind every threshold written down.

This file is the register. It is not a summary of a register held somewhere
else. If a measure is not in this table it is not registered, and a run that
does not read it is not omitting anything; that equivalence is the whole point
of the file and is what the guard exists to keep true.

## Why this is tracked, and what went wrong when it was not

Until issue #2961 the register was held privately by the scoring harness.
Thresholds were registered, amended, and retired with no diff, no review, and
no record here. Two failures followed, and only the second was survivable.

Measure **M5** was registered for runs 11 and 12 at `attempted >= 200`. Issue
#2879 showed that bar was unreachable at the moment it was registered: the
remedy's own constants bound the per-tree count at 16, and run 12 went on to
measure exactly 16 across a ten-tree cohort. That is a **wrong threshold**, and
a wrong threshold is loud. It scores `FAIL`, somebody reads the `FAIL`, and the
arithmetic gets done.

Between run 12 and run 14, M5 was not corrected. It was **dropped**. No M5, no
`blocked_leaf_reactivations` measure, no threshold of 200 anywhere in the run-14
predicate. That is silent. The subject went unmeasured, the work it covered
shipped with no reading, and nothing anywhere recorded that a reading was owed.

The two failures are not equally bad, and the register is shaped around the
difference:

| artefact | reports | actually means |
|---|---|---|
| an absent measure | the run passed | the measures still in the register passed |
| an unreachable threshold | the run failed | somebody must check the arithmetic |

An absent measure is indistinguishable from a measure that was never owed. That
is the same defect this epic keeps finding in instrumentation - an absent series
reads as a measured zero - reappearing in the harness that scores the fixes for
it. Tracking the register makes a removal a deletion in a diff, and makes
"200 was unreachable when it was registered" a claim a reader can contradict.

## How a row is checked

`AcceptanceMeasureRegisterTests` in `test/lattice/Hygiene/` reads this file and
fails the build on any of the following. It is a gate over the register, not
over a run: it can prove a threshold is unreachable, and it cannot prove a run
took the reading.

1. The file is missing, has no parseable rows, or has a row with the wrong
   number of cells.
2. A cell that must carry a value is empty.
3. `Instrument` does not name an instrument the core assembly declares.
4. `Arm source` does not resolve to a pre-allocated tag field, or resolves to a
   tag whose key and value are not the ones `Arm` states. The two columns are
   independent statements of the same fact and must agree, so renaming an arm
   in source reddens the register rather than quietly orphaning it.
5. `Ceiling derivation` does not evaluate to `Ceiling`.
6. A constant named in a derivation does not exist, or the derivation's value
   moved because a constant's value moved.
7. A lower-bound `Threshold` exceeds `Ceiling`, which is the exact shape of the
   M5 defect. **Registering `>= 200` against a derived ceiling of 16 cannot be
   committed.**
8. A row whose `Status` is `retired` does not record the run and the reason.
9. The scan matched nothing, at any stage. A guard that silently finds no work
   reports the same green as one that checked everything.

### What it does not check

Stated plainly, because a gate whose coverage is unstated will be assumed
total.

- **It does not know which measures a run actually read.** The predicate lives
  in the harness. This file records what is owed; nothing here observes whether
  a run paid it. A measure dropped from the harness while left in this table is
  still invisible, and closing that needs the harness to publish the measure ids
  it evaluated.
- **It does not check a `n/a` ceiling.** A measure with no structural ceiling is
  carried with a stated reason and its threshold is unverified arithmetic. That
  is a real hole and it is why `n/a` is permitted only with a reason.
- **It does not check the register is complete.** No mechanism can tell this
  file that a measure exists which nobody wrote down. Completeness is a human
  obligation, and the seeding note below says exactly how far it currently
  extends.

## Column semantics

| column | meaning |
|---|---|
| `Measure` | The measure id the run predicate uses. |
| `Status` | `gating` (a failure fails the run), `diagnostic` (read but not gating), or `retired`. |
| `Instrument` | The dotted instrument name, checked against the instruments the core assembly declares. The Prometheus exposition renames these, so `orleans.lattice.wal.gc.blocked_leaf_reactivations` is read as `orleans_lattice_wal_gc_blocked_leaf_reactivations_total`. |
| `Arm` | The tag selecting the series, as `key=value`. |
| `Arm source` | The `Type.Member` in the core assembly that pre-allocates that tag. Resolved by reflection and required to agree with `Arm`. |
| `Threshold` | The registered bar, as a comparator and an integer, or `n/a`. |
| `Ceiling` | The structural maximum the remedy's own constants permit, or `n/a`. |
| `Ceiling derivation` | An arithmetic expression over named source constants that must evaluate to `Ceiling`; or, when `Ceiling` is `n/a`, the reason there is no structural ceiling. |
| `Registered` | The run the measure was first registered for. |
| `Retired` | `-` while live, otherwise `run N: reason`. |

### The derivation expression language

Integer literals, `Type.Member` references, `+`, `-`, `*`, `/`, and parentheses,
with the usual precedence and truncating integer division. A referenced member
may be an integer constant or a `TimeSpan`; a `TimeSpan` resolves to its whole
minutes, because every rate bound in this subsystem is expressed per minute.
Types are resolved in the `Orleans.Lattice` assembly, and a member may be
`private` - the guard binds non-public statics, so no visibility is widened to
satisfy it.

The point of the expression is not that it is short. It is that **a threshold is
a claim about the system**, so it has to be written as arithmetic over the
system's own constants rather than as a number. M5's 200 would not have survived
this column, because writing the derivation down is what does the arithmetic.

## The register

| Measure | Status | Instrument | Arm | Arm source | Threshold | Ceiling | Ceiling derivation | Registered | Retired |
|---|---|---|---|---|---|---|---|---|---|
| M5 | gating | `orleans.lattice.wal.gc.blocked_leaf_reactivations` | `outcome=attempted` | `LatticeMetrics.BlockedLeafReactivationAttempted` | `>= 16` | `16` | `LatticeWalGc.MaxReportedBlockingConsumers * (1 + (30 - LatticeWalGcScheduler.ReactivationMinBlockAge) / LatticeWalGcScheduler.ReactivationRetryCooldown)` | run 11 | - |
| M6 | diagnostic | `orleans.lattice.wal.gc.blocked_leaf_reactivations` | `outcome=undelivered` | `LatticeMetrics.BlockedLeafReactivationUndelivered` | `n/a` | `n/a` | Denominator guard, not a bar: `attempted - undelivered` is a count, and its zero is the finding rather than a failure. | run 12 | - |
| A10.1 | diagnostic | `orleans.lattice.wal.gc.blocked_leaf_reactivations` | `outcome=attempted` | `LatticeMetrics.BlockedLeafReactivationAttempted` | `> 2` | `n/a` | Empirical per-tree baseline measured on run 11, not a structural maximum, so no derivation exists and the bar is unverified arithmetic. | run 11 | - |

### M5

The registered bar is `attempted >= 16` and the derivation reads:

```
MaxReportedBlockingConsumers * (1 + (window - ReactivationMinBlockAge) / ReactivationRetryCooldown)
      8                      * (1 + (  30  -           5            ) /          15              )
      8                      * (1 +                    1                                          )
                                       16
```

`window` is the 30-minute scoring window, and is the one literal in the
expression that is not a source constant; it is a property of the run, not of
the remedy. A consumer becomes eligible after `ReactivationMinBlockAge`, then
one further attempt is permitted per `ReactivationRetryCooldown`, giving two
attempts inside the window. At most `MaxReportedBlockingConsumers` consumers are
reported per pass. Hence 16, which is what run 12 measured on every tree in a
ten-tree cohort spanning two tenants and three naming families.

The previously registered 200 was never reachable. Even ignoring the
per-consumer rate limit entirely, the absolute bound is
`MaxReactivationTouchesPerPass * blocked passes`, which on run 11's 46 blocked
passes is `4 * 46 = 184`. No behaviour of the remedy could have cleared 200.
The error was reading run 11's `attempted = 2` as a baseline a working fix would
lift, when 2 was **already the saturated ceiling** at one reported blocker per
pass. The remedy raises the reported blockers from 1 to 8, so the honest
expected multiple is about 8x and 8x is simultaneously the maximum. A bar of
100x was never describing the remedy.

History: registered `>= 200` for runs 11 and 12 (#2879 shows it was unreachable
when registered); absent entirely from the run-14 predicate (#2961); reinstated
here at the derived ceiling.

### A10.1

A10.1 and M5 read the same counter and the same arm and are **not** the same
threshold. M5 asks whether the sweep reached its designed rate. A10.1 asks
whether the scored tree is dead while its siblings are alive, firing on
`self > 2` against the run-11 per-tree constant.

Retuning A10.1 to 16 alongside M5 would destroy it: run 12's entire cohort reads
16, so `self > 16` is false for every tree and A10.1 could never fire again. The
two were conflated once already, and the conflation was caught only by reading
the clauses. **A search for the counter finds both; only reading them separates
them.** Any sweep for "what else depends on this quantity" has to key on which
comparison a clause makes, not on which counter it reads.

Open, deliberately not decided here: run 12 moved the per-tree baseline from 2 to
16, so if the next run re-tests reach, A10.1's comparator arguably becomes 16;
if it tests delivery instead, the deadness signal is no longer `attempted` at all
but `healed` and `undelivered`, and A10.1 should key on a different arm. Leaving
it at 2 makes A10.1 structurally unable to fire against a cohort sitting at 16.
That may be correct, because there would be no anomaly left to detect, but it
should be a decision rather than an inheritance.

## Seeding, and the limit of this table

This register was seeded from the measures recoverable from issues #2879 and
#2961, which are M5, M6 and A10.1. **It is not asserted to be the complete set
of measures any run was scored against**, and nothing here can make it so: the
run-11 through run-14 predicates were never tracked, so measures they carried
and nobody wrote down are unrecoverable by reading the repository.

Stating that is not a disclaimer, it is the same distinction the register is
for. A table that quietly presented three rows as the whole population would be
the untracked register's defect with a file extension. Whoever holds the current
predicate must add the rest before the next scored run, and adding one is a
diff.

## Registering, amending, and retiring a measure

- **Register**: add a row. Derive the ceiling from source constants before
  choosing the threshold, not after. If no structural ceiling exists, write
  `n/a` in both `Ceiling` and `Threshold` only when the measure genuinely has no
  bar, and put the reason in the derivation cell.
- **Amend**: change the row. The diff is the record, so no separate note is
  needed, but say why in the commit message.
- **Retire**: set `Status` to `retired` and fill `Retired` with `run N: reason`.
  **Do not delete the row.** A deleted row is exactly the silent drop this file
  exists to prevent, and the guard cannot tell a deletion from a measure that
  never existed.
