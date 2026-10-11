# TreeLifecycle mutation experiments

Each `.mutation` is an exact edit of the current `TreeLifecycle.tla`, not a
frozen copy of the module. `PERTURBS` identifies the changed action. `TARGET`
names the invariant or temporal/action property that must detect the edit.
The Formal catalogue gates check both property and action coverage and refuse
ambiguous or stale anchors.

The experiments cover sparse source enumeration, omitted copying and fencing,
early swaps, undo redirect order and fence clearing, retained mirror interlocks,
grow/shrink completion and physical binding after a cutover, restore/revert
targets and redirect completion, reservation loss, deletion admission, invalid
retry state, and stranded progress. A mutant failing `TypeOK` instead of its
named safety property is not accepted.

The TLC harness checks a clean single-property control before every distinct
target/configuration group and shares it only inside one test process. All
TreeLifecycle cases stay in `TlcShardTreeLifecycle`, so this control reuse is
preserved while other formal areas run on separate CI work items.
