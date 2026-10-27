# Atomic commit without consensus - storyboard

What is on screen for each cue of [SCRIPT.md](SCRIPT.md). The diagram
separates the staged values from the registry decision that makes their
outcome visible.

| Scene | Component | Cue | On screen |
| --- | --- | --- | --- |
| One atomic decision | `title-card` | 1 | The title, "Atomic commit without consensus", and "One decision, two keys". |
| Prepare and decide | `atomic-saga` | 2 | Two keys, `order/status` and `order/tracking`, sit in separate leaf cards. |
| | | 3 | The coordinator sends prepared writes to the leaves that own the keys. |
| | | 4 | Pending values appear in both leaves, labelled as hidden from readers. |
| | | 5 | The transaction registry changes from "In flight" to "Commit recorded". |
| | | 6 | The registry is the single tree-wide visibility point. |
| | | 7 | A reader resolves a pending key through the registry's outcome. |
| | | 8 | The reader's batch view changes from "No batch effect" to "Whole batch". |
| | | 9 | Terminal marks travel after the decision; a read can follow the registry outcome while marks are in flight on the originating tree. |
| | | 10 | Both leaves show the outcome applied; neither chooses it independently. |
| Commit or abort | `choice-list` | 11 | The commit and abort outcomes appear as two branches. |
| | | 12 | Abort drops prepared values without issuing per-key rollback writes. |
| | | 13 | The note explains why a rollback must not overwrite a concurrent write. |
| | | 14 | The coordinator drives the saga and learns whether the shard fan-out staged the batch. |
| | | 15 | The registry's recorded outcome, rather than consensus among leaves, decides what readers see. |
| | | 16 | "TTL and soft delete" is named as the next Build episode. |
| | | 17 | "Verified atomic commit" appears as the introduced page. |
