# Schema and view composition

`SchemaViewComposition` composes the schema modules with an independent numeric
(Count/Sum) projection. It is the bounded check of the projection input contract
settled for #4908: on a versioning-enabled source, a projection receives the
envelope-stripped body at its stamped version, never upcast, on every derivation
path, and a target advance re-projects nothing. Refreshing contributions to a
new shape takes a projection change (and its rebuild) or a rewrite through the
WAL: an eager migration or a transform remediation.

The module lives in its own directory so that editing it reruns only itself,
and changes the fingerprint of neither `spec/schema/` nor the materialised-view
areas. It instances no module of either side. The schema write path, the version
family, strict ingest, the remediation cutover and the numeric maintainer each
appear as the environment their own checked modules guarantee, and the
refinement note's `## Relied-on guarantees` table names the module whose
properties discharge each assumption.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `SchemaViewComposition` | 7 | 3 | 15 | 23 | 23 | 148,514 |

## What is modelled

Two source keys follow a fixed script: a raw write, a write that an accepting
stage transforms or the policy refuses, and an overlapping two-key atomic batch
the environment commits or aborts. Each value is stored with the version it was
stamped at. A version changes what a body's shape means - version 2 regroups
one body, rescales another's numeric and puts a third outside the filter - so an
upcast changes a contribution's group, numeric or filter membership. Projection
definition 1 reads only the version-1 shape; definition 2 reads both.

The environment advances the target, replaces the projection definition,
delivers one value below the check with a newer source stamp (diverted under
strict ingest when the target cannot decode it), retries a write whose response
was lost, and runs one rewrite. The rewrite reads every value in its stored form
and either aborts on one it cannot decode, changing nothing, or writes each
value re-stamped through the destination copy's ordinary write path and cuts
over by moving the alias. The view applies each WAL entry through the
projection-input seam, publishes a decided batch atomically, replays retained
entries from its checkpoint after a crash, rebuilds from the bound copy's stored
values when its definition changes or the alias moves, and serves a head
barrier that returns only once it is bound to the copy the alias names.

## Instances

| Instance | Rewrite | Strict ingest | Retries | Faults | Reconciles | Checked |
|----------|---------|---------------|---------|--------|------------|---------|
| Base | Eager migration | No | 0 | 1 | 0 | Every property |
| `Remediate` | Transform remediation | No | 0 | 0 | 1 | Every property |
| `Strict` | Eager migration | Yes | 1 | 1 | 0 | Invariants and action properties |

The distinct-state census of each instance is recorded in the module's manifest.
Liveness at the `Strict` bound is classified bounded-out for the TLC budget;
`Converges` is checked on the base and on `Remediate`.

## Scope and limits

The model is a bounded intended protocol, not an implementation proof. It fixes
two versions, three body shapes, one upcaster and one transform; it runs one
silo, one rewrite and no write while the rewrite builds; and it relies on the
numeric companion for publication obligations, checkpoint banking and operation
identity. The [refinement note](SchemaViewCompositionRefinement.md) maps every
action and property to its production seam and caller-level detectors, and its
gap register records every limit.

## How to run

From this directory, with the pinned toolchain described in the
[spec README](../README.md#how-to-run-tlc):

```text
java -cp <repo>/tools/tlc/build/compiled:<repo>/tools/tla2tools.jar tlc2.TLC -config SchemaViewComposition.cfg SchemaViewComposition.tla
```

Through the NUnit harness, every base, variant and mutation case of this module:

```text
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "TestCategory=TlcModule_SchemaViewComposition"
```
