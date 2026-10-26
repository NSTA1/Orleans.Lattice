# Values that merge - script

Status: fact-checked on 2026-10-26 by the Docs agent, all 21 cues and 14 Sources
rows, against the live CRDT guides, figure registry and implementation. Two
corrections applied: the recap registers Lattice on a silo, not the silo itself;
accessors encode the CRDT delta, while some still accept caller payload bytes.
The narrated surface was also checked against the lattice-v9.9.0 release tag.

Written form, in the series voice (Emma, British English). This is B2 on Build,
covering the Orleans.Lattice 9.9 core package. It opens with a one-line recap.
No code is shown; the CRDT guide holds the compiled accessor examples.

## Narration

<!-- pause 1.0 -->

### Values that merge

In Hello, Lattice, you registered Lattice on a silo and wrote and read typed values.

Now choose what should happen when two writers change the same key concurrently.

### Replace or merge

A plain set keeps the last write. It does not combine the contents of two records. If both updates matter, replacing one is the wrong rule.

A conflict-free replicated data type makes that rule part of the value. Choose the rule that matches your data, not just the shape of its fields.

### A counter keeps both

For a total that only grows, choose a grow-only counter. Each writer advances its own component.

Here, cluster A adds three, while cluster B adds five. Neither has seen the other's change.

Merge takes the larger component for each writer. Both components survive, and the value is their sum: eight.

Deliver the same state again and the value stays eight. For a count that can also decrease, choose a positive-negative counter: separate totals for increments and decrements.

### Choose a set

For a collection that only gains members, a grow-only set combines the members from both sides.

If members can be removed, choose what a concurrent add and remove should mean. An observed-remove set keeps the add.

A remove-wins set makes the opposite choice. That choice is part of the data's meaning, not a setting to leave until a conflict happens.

### Keep the conflict visible

Sometimes neither write should disappear. A multi-value register keeps values written concurrently.

Both clusters started from todo. One writes in progress, and the other writes resolved, without seeing the other's write.

The merged register holds both. Your application reads the candidates and decides how to resolve them.

A later write, made after observing both values, replaces both. Two writes in a row on one cluster are not this conflict: the second sees the first.

### Registers and maps

For a value that should only move upwards, choose a maximum register; for one that only moves downwards, a minimum register.

For a dictionary whose values also need to merge, choose an observed-remove map. Its keys are add-wins, and each value merges by its own conflict-free rule.

### Use the typed accessor

The typed accessor chooses the merge mode and encodes the delta. You do not hand-encode the CRDT delta. Give independent writers stable, distinct identities.

A local tree can mix these types on different keys. A replicated tree must use the single merge mode declared in its replication map.

Maps also need their key and value shape registered on the host, once per tree. The CRDT guide has the accessor examples and the details for each choice.

### Where next

Next on Build: Scans, filters and cursors. The deep dive on how this works is Conflict-free merges in 90 seconds. The links are on this episode's page in the documentation.

<!-- pause 2.0 -->

## Sources

| Narration | Source |
| --- | --- |
| recap: register, write and read | B1 `videos/episodes/hello-lattice/SCRIPT.md`; `videos/series.md`, B1 |
| plain set replaces rather than combining records | `docs/crdt/readme.md`, "What is a CRDT" warning and last-writer-wins note |
| choose the rule to match the data | `docs/crdt/readme.md`, "The catalogue at a glance" and "The primitives" |
| grow-only counter, per-writer components, sum and re-delivery | `docs/crdt/gcounter.md`, "Semantics"; `docs-site/figures/join-figures.json`, `gcounter` scenario, displayed without restating its labels |
| positive-negative counter for increments and decrements | `docs/crdt/pncounter.md`, "Semantics"; CRDT overview's counter family |
| grow-only set combines members; add-wins and remove-wins | `docs/crdt/gset.md`, `orset.md`, `rwset.md`, "Semantics"; CRDT overview's choice table |
| multi-value register keeps concurrent writes and application resolves them | `docs/crdt/mvregister.md`, "Semantics" and "Example"; site's `mvregister` scenario |
| observed later write replaces both; sequential writes do not conflict | `docs/crdt/mvregister.md`, "Semantics" and the explicit warning in "Example" |
| monotone maximum and minimum registers | `docs/crdt/readme.md`, register family; `maxregister.md` and `minregister.md` |
| map keys add-wins and values merge recursively | `docs/crdt/ormap.md`, "Semantics" |
| typed accessor encodes delta; independent writer identities | `docs/crdt/readme.md`, "How you use them here" |
| local tree mixes modes, replicated tree declares one mode | `docs/crdt/readme.md`, cross-cluster replication note |
| map shape registered once per tree | `docs/crdt/ormap.md`, "Semantics" and host wiring in "Example" |
| closing titles and handoff | `npm run series -- ending B2`, verbatim |
