# TTL and soft delete - storyboard

What is on screen for each cue of [SCRIPT.md](SCRIPT.md). The title card and
camera notation come from shared components and `shared/brand/brand.css`.
The code panels are copied from the compiled snippets on the companion page.
Clip windows and beats are stamped from the narration by `npm run timeline`.

| Scene | Component | Cue | On screen |
| --- | --- | --- | --- |
| TTL and soft delete | `title-card` | 1-2 | The Build title card. The recap names atomic writes, then the title and the two ways data goes away. |
| Entries that expire | composition | 3-5 | The compiled snippet writes a session with a thirty-minute time to live. A note shows the silo stamping an absolute expiry. Once it passes, every read path is listed as not seeing the entry. |
| Refresh and reaping | composition | 6-8 | Two cards: a winning plain write clears the expiry; a CRDT write merges expiries by the later one. A note then shows compaction reaping the entry after the tombstone grace period. |
| Soft delete | composition | 9-10 | The compiled delete snippet. Beside it, the tree's state: inaccessible at once, reads and writes throw, data kept for 72 hours by default. |
| Recovery | composition | 11-12 | The compiled recover snippet. Beside it, three states: inside the window the tree comes back with its data; purge in progress and purge complete are too late. |
| Where next | composition | 13 | A filled node names Going durable as next on Build. A hollow node names the TTL documentation page. |
