# Values that merge - storyboard

Timing is stamped from the narration, never entered here.
All diagrams use the site's notation and labels.

| Cue | Scene | Picture and change |
| --- | --- | --- |
| 1-2 | Values that merge | Shared title card, Build / B2 / Orleans.Lattice 9.9. The spoken recap precedes the question about concurrent writes. |
| 3-4 | Replace or merge | Shared choice list: last-write replacement, then a merge rule belonging to the value. The marker identifies the current choice. |
| 5 | A counter keeps both | Shared join diagram, the site's `gcounter` scenario, starting with the hollow bottom and the order. |
| 6 | A counter keeps both | A adds three and B adds five, level as concurrent states. |
| 7 | A counter keeps both | Both deltas travel upwards; the join fills in marker yellow with value eight. |
| 8 | A counter keeps both | Re-deliver B; the join stays eight. Narration distinguishes PN-Counter for decrements. |
| 9-11 | Choose a set | Shared choice list: grow-only, add-wins, remove-wins, one choice revealed per cue. |
| 12 | Keep the conflict visible | Shared join diagram, the site's `mvregister` scenario, with todo at the bottom. |
| 13 | Keep the conflict visible | The two concurrent writes appear: in-progress and resolved. |
| 14 | Keep the conflict visible | Both paths meet at the join, retaining both candidates. |
| 15 | Keep the conflict visible | Hold the conflict set while narration explains a later observing write and why two sequential writes do not create it. |
| 16-17 | Registers and maps | Maximum/minimum move in one direction; map values merge recursively. No unported chain scenario is used. |
| 18-20 | Use the typed accessor | Typed deltas and writer identities; local mixed modes versus replicated single mode; host map-shape registration. |
| 21 | Where next | Name B3 and H1 by the plan's exact titles, then the CRDT primitives guide. The companion holds the links. |

## Shared material

`title-card` and `join-diagram` are unchanged. `choice-list` adds a variable-driven
list of at most three choices, with one entry per narrated beat; it is shared
because the same scene structure is useful to any episode comparing rules.
No code is shown, no colours or fonts are copied, and the caption band stays
clear.
