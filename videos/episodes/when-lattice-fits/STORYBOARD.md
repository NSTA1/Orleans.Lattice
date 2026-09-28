# When Lattice fits, and when it doesn't - storyboard

What is on screen for each cue of [SCRIPT.md](SCRIPT.md). The opening is the
shared `title-card`; the store, the cluster and the seams are the shared
`store-overview`, `cluster` and `seams`; every other scene is laid out in the
composition itself, in the series' notation: a node per thing, on a spine, a
hairline frame for what holds it, and mono only for keys and values. The
plain-words scene uses the same keys (`basket/ada`, `order/1001`) that the
technical scenes draw. Every clip's window and every component's beats are
stamped from the narration by `npm run timeline`.

| Scene | Component | Cue | On screen |
| --- | --- | --- | --- |
| When Lattice fits | `title-card` | 1 | The mark and "Evaluate"; "When Lattice fits, and when it doesn't" as the title; "What it takes the place of, and where it does not fit". |
| What a shop remembers | composition | 2 | "What a shop remembers". On a spine, two filled nodes: "A basket", `basket/ada 3 items`; "An order", `order/1001 paid`. |
| | | 3 | Beside them, three separate hairline frames: "A database", to keep it; "A cache", to read it quickly; "A queue", to pass work along. |
| | | 4 | In each frame, its own idea of the basket: `3 items`, `2 items`, `add 1 item`; beneath, a hollow node and "Something has to keep them in agreement". |
| | | 5 | The frames and the list give way to one frame, "The application", round one store, "One store, one set of rules", holding the same two keys. |
| | | 6 | Three places side by side, "The website", "The phone app", "The warehouse", each with `basket/ada 3 items`; beneath, a filled node and "Every place, the same answer". |
| | | 7 | Two of those places, at the same moment, give the one plain value two answers: `4 items` and `2 items`. "Only one change is kept", then, beneath, "unless the value is one of the kinds that merge". |
| What it is | `store-overview` | 8 | "A sorted key-value store, in your cluster"; the cluster's frame draws. |
| | | 9 | The keys draw on one chain, in order: `basket/ada`, `basket/bo`, `order/1001`, `order/1002`. As the cue ends, "External database", "Coordinator" and "Queue" arrive outside the frame and are struck. |
| | | 10 | The store gives way to the three positions on a spine: "The store lives in the cluster", "Conflict resolution is algebraic", "Everything else is a seam". |
| The store lives in the cluster | `cluster` | 11 | "The store lives in the cluster": the cluster's silos, the grains holding the keys, your code beside them. |
| | | 12 | A read drawn as a grain call; outside the frame, a pale "Separate database tier", its own scaling, its own failures, dismissed. |
| | | 13 | Beneath the dismissed tier, a filled node: "A read cache on each silo", refreshed on every read, by default. |
| Conflict resolution is algebraic | composition | 14 | "Conflict resolution is algebraic". "A merge is", and three filled nodes: "Commutative", in any order; "Associative", in any grouping; "Idempotent", any number of times. |
| | | 15 | Beneath them, "No distributed lock manager" and "No consensus round trip", pale and struck; then a filled node and "Any cluster can accept a write to any key". |
| Everything else is a seam | `seams` | 16 | The site's "A core plus seams": the core, then one node per released section of PACKAGES.md, each with its count and first packages. |
| | | 17 | A host registers Storage, Identity and Security, and Replication; the sections it leaves out empty and fade. |
| What it composes into | composition | 18 | "What it composes into". "The core alone", on a spine: point reads and writes, ordered scans, atomic writes across many keys, typed queues, a distributed lock, a saga coordinator. |
| | | 19 | Beside it, "Composes into": knowledge systems, digital twins, distributed control planes, platforms with many tenants, collaborative applications. |
| | | 20 | Both give way to a filled node and "A platform rather than a product", then "Not tied to one kind of application" and "How it is deployed: a configuration decision, taken late". |
| Where it does not fit | composition | 21 | "Where it does not fit", "As plainly", and an empty spine. |
| | | 22 | The first row: "It needs an Orleans cluster", without one, it has nowhere to run. |
| | | 23 | "Concurrent writes to a plain value are not merged": the later clock wins; to keep both, use a value that merges. |
| | | 24 | "No single order across atomic writes", and no one read across several trees at the same instant. |
| | | 25 | "A value has to fit in one entry of its log": store a very large value elsewhere, and keep a reference. |
| | | 26 | "The default log is in memory": give the log and the grain storage a durable home. All five rows stay. |
| Where next | composition | 27 | "Where next". A filled node: "The guarantees", "Next on Evaluate". |
| | | 28 | Two hollow nodes beneath: "What it is and why it exists", the three positions; "A core plus seams", the packages behind them. |
