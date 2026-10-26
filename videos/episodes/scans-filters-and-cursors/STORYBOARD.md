# Scans, filters and cursors - storyboard

What is on screen for each cue of [SCRIPT.md](SCRIPT.md). The title card and
camera notation come from shared components and `shared/brand/brand.css`.
Every code panel is copied from the compiled snippets on the companion page.
Clip windows and beats are stamped from the narration by `npm run timeline`.

| Scene | Component | Cue | On screen |
| --- | --- | --- | --- |
| Scans, filters and cursors | `title-card` | 1-2 | The Build title card. The opening recap names the merge rule, then the episode's title and subject. |
| An ordered range | composition | 3-5 | Three lexicographically ordered keys; the included lower bound and excluded upper bound frame the range, then the direction turns around. |
| Filter at the leaf | composition | 6-11 | A value predicate travels to its owning leaf. A list of candidate ages narrows to matches. The bounded scan snippet shows a typed predicate and an explicit key range. |
| Resilient streams | composition | 12-14 | A streaming walk pauses when its remote enumerator is reclaimed, resumes at the last key with the filter intact, and either completes or reports exhausted recovery. |
| Pages that survive | composition | 15-18 | The compiled cursor snippet opens, reads and closes pages. A server-side bookmark is paired with the caller's saved cursor ID; a point-in-time label distinguishes one saga-decision view from live pages. |
| Where next | composition | 19-20 | A filled node names Atomic writes as next on Build. Two hollow nodes name Predicate operations and the API reference. |
