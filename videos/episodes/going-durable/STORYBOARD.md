# Going durable - storyboard

What is on screen for each cue of [SCRIPT.md](SCRIPT.md). The title card and
camera notation come from shared components and `shared/brand/brand.css`.
The code panels are copied from the compiled snippets on the companion page.
Clip windows and beats are stamped from the narration by `npm run timeline`.

| Scene | Component | Cue | On screen |
| --- | --- | --- | --- |
| Going durable | `title-card` | 1-2 | The Build title card. The recap names TTL and soft delete, then the title and the two durable replacements. |
| Two halves, both in memory | composition | 3-4 | The compiled registration of the in-memory defaults. Then two cards: grain storage holds leaves and their snapshots; the write-ahead log holds every committed write, in order. Both read "In memory: lost on restart". |
| Durable grain storage | composition | 5-6 | The compiled Azure Table grain storage registration through `AddLattice`, full width. Under it, a row of facts: the provider; then a write with a stale ETag must fail, and a provider that accepts one stops the silo from starting, by default. |
| A durable log on disk | composition | 7-8 | The compiled file write-ahead log registration, full width. Under it, a row of facts: local disk with no cloud dependency; then a single container with a mounted volume, and each batch all or nothing. |
| Or in Azure Table storage | composition | 9-10 | The compiled Azure Table write-ahead log registration, full width. Under it, two facts: production replicated across regions, before or after `AddLattice`. Then a note: the log is only half of a durable tree, trimmed once a grain storage snapshot covers it, so pair it with durable grain storage. |
| Where next | composition | 11 | A filled node names Moving in from another store as next on Build. Hollow nodes name the four documentation pages it introduces. |
