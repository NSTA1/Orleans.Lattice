# Conflict-free merges in 90 seconds - storyboard

What is on screen for each cue of [SCRIPT.md](SCRIPT.md). Every scene uses a
shared component, and `npm run timeline` stamps its beats from the narration.

| Scene | Component | Cue | On screen |
| --- | --- | --- | --- |
| Opening | `title-card` | 1 | Kicker "How it works", title "Conflict-free merges in 90 seconds", subtitle "Two additions. One union." The recap is spoken over the title. |
| G-Set merge | `join-diagram`, `gset` | 2 | The empty set and the heading "A G-Set merges by union". The narration contrasts plain last-writer-wins writes. |
| | | 3 | The lower edges draw as Cluster A adds `alice` and Cluster B adds `bob`; their states appear as concurrent. |
| | | 4 | The upper edges draw; both deltas reach `{alice, bob}`, the union at the join. |
| | | 5 | Bob's delta is delivered again; the join remains `{alice, bob}`. |
| | | 6 | The note appears: "Commutative, associative and idempotent" and "Append-only membership". |
| | | 7 | The joined state and note hold as the narration names the G-Set use case and removal alternatives. |
| | | 8 | The diagram and note hold through the ending, then the scene exits. |
