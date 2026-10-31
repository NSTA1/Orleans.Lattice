# Clocks and version vectors - storyboard

What is on screen for each cue of [SCRIPT.md](SCRIPT.md). Scenes use shared
components, and `npm run timeline` stamps their beats from the narration.

| Scene | Component | Cue | On screen |
| --- | --- | --- | --- |
| Opening | `title-card` | 1 | Kicker "How it works", title "Clocks and version vectors", subtitle "Causal order, one replica at a time". The recap is spoken over the title. |
| | | 2 | The title holds as the narration asks how replicas compare histories. |
| Causal history | `version-vector` | 3 | "Causal history, entry by entry" and "A wall-clock timestamp is not proof of cause". |
| | | 4 | The caption explains that a version vector keeps one clock for each replica. |
| | | 5 | The two histories appear as rows, with each replica's own entry and the entry it has observed. |
| | | 6 | A's vector is `A=2, B=1`; B's is `A=1, B=3`. |
| | | 7 | The two rows are marked concurrent: each is ahead in a different replica entry. |
| | | 8 | The merged row appears as `A=2, B=3`, with the join marker; a repeated merge leaves it unchanged. |
| | | 9 | The dominance note appears: every entry is at least as high as the compared history. |
| | | 10 | The diagram gives way to the compiled accessor example: tick a replica, read the vector and compare it. |
| | | 11 | The code holds as the narration leaves conflict resolution to the application. |
| Where next | `title-card` | 12 | "Back on Evaluate" and "The evidence", held through the closing narration. |
