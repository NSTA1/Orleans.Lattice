# Atomic writes - storyboard

What is on screen for each cue of [SCRIPT.md](SCRIPT.md). The title card and
camera notation come from shared components and `shared/brand/brand.css`.
The cross-tree code panel is copied from the compiled snippet on the companion
page. Clip windows and beats are stamped from the narration by
`npm run timeline`.

| Scene | Component | Cue | On screen |
| --- | --- | --- | --- |
| Atomic writes | `title-card` | 1-2 | The Build title card. The recap names the previous episode's read path, then the title and the all-or-nothing promise. |
| The partial-write problem | composition | 3-5 | Two related order fields appear as separate writes. The first becomes visible before the second fails; a non-atomic batch is labelled as allowing partial application. |
| One batch, one decision | composition | 6-9 | A single-tree batch prepares two hidden changes. One decision reveals both together, or leaves the prior state visible; timeout is distinguished from an abort. |
| Across trees | composition | 10-12 | The compiled cross-tree snippet moves an order between two trees. Both slices meet at one shared decision, with the stable operation identifier visible. |
| The boundary | composition | 13-14 | Two separate reads sit on opposite sides of the decision: one can show the old state, the next the new. A line marks this as write atomicity, not a read snapshot. |
| Where next | composition | 15 | A filled node names TTL and soft delete as next on Build. Hollow nodes name Atomic commit without consensus and the Atomic writes documentation page. |
