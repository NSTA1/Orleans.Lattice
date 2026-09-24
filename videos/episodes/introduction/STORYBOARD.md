# Orleans.Lattice in three minutes - storyboard

What is on screen for each cue of [SCRIPT.md](SCRIPT.md). Every scene is a
shared component under `shared/components/`; the words on screen come from the
site where the site has them, and each change lands on a cue's beat, stamped
from the narration by `npm run timeline`.

The first three scenes are for everyone: plain words, one everyday example,
and the same notation the technical half uses, so the pictures a viewer learns
here are the ones they meet again later.

| Scene | Component | Cue | On screen |
| --- | --- | --- | --- |
| Opening | `title-card` | 1 | The mark and "Orleans.Lattice"; the site's thesis as the title (`site:home.thesis`); "An introduction in three minutes". |
| Software that remembers | `remember` | 2 | "What an application remembers": three nodes on a spine, each an everyday thing - what is in a basket, who may open a document, how often a page is viewed - arriving as it is named, with the value the application keeps beside it in mono (`basket/ada 3 items`, `document/plan 4 readers`, `page/views 20`). |
| | | 3 | A frame closes round the values, labelled "state"; the descriptions step back to secondary ink. |
| In more than one place | `many-places` | 4 | Two places, "Place A" and "Place B", each a frame holding its copy of `page/views 20`. As "if one place fails" is said, Place B dims and Place A keeps going. |
| | | 5 | Place B returns. Both change the value at the same moment: A counts 3 more views (23), B 5 more (25); both nodes turn concurrent blue, joined by a dashed line, "at the same moment". |
| | | 6 | The usual answer: B's change "waits its turn" and its node empties; as "a separate database" is said, a pale node of that name appears below, with long dashed round trips to both places, and the values inside the places fade. |
| | | 7 | The heading becomes "A different approach"; as "inside the application itself" is said, the database and its round trips go, and the memory is back inside both places; B's change is live again. |
| | | 8 | Each change travels to the other place; on arrival both read 28, and both nodes fill with the marker - the join - and pulse. "The same count in both places, and neither waits." |
| | | 9 | The places give way to an ink chain: "One machine" (no cloud account) to "Many regions" (without a rewrite). |
| What it is | `store-overview` | 10 | "A sorted key-value store, in your cluster": the frame "Your Orleans cluster" appears as the platform is named. |
| | | 11 | Sorted keys draw in as nodes on one chain inside the frame. Outside it, three pale nodes - external database, coordinator, queue - arrive and are struck through as they are named. |
| | | 12 | The store gives way to the three positions, each a node on a spine. |
| The store lives in the cluster | `cluster` | 13 | The cluster frame with three silos as spines; grains as hollow nodes labelled with keys, your code as a filled node; a grain call drawn in ink from the code to a grain on the next silo. |
| | | 14 | A separate database tier appears outside the frame with a long dashed round trip, then fades; the grain call pulses. |
| Conflict resolution is algebraic | `join-diagram`, `gcounter` | 15 | The site's G-Counter figure at rest: the order in pale edges. |
| | | 16 | The lower edges draw in ink as the writes rise: A adds 3, B adds 5, each to its own count; "concurrent" as the cue closes. |
| | | 17 | The upper edges draw as each merges the other's update; the join fills with the marker and pulses: A=3, B=5, value 8. |
| | | 18 | B's delta travels to the join again, and nothing changes. |
| | | 19 | "Commutative, associative and idempotent" and "No lock manager, no consensus round trip" under the heading. |
| Everything else is a seam | `seams` | 20 | The site's core-plus-seams list, generated from `PACKAGES.md`: the core, then each section with its package count and first packages, on a spine. Released packages only, without the Explorer. |
| | | 21 | A host registers Identity and Security, Replication and Storage: those stay filled in ink; the seams it leaves out empty and fade. |
| Local to Global | `journey` | 22 | The site's journey on one ink chain; Local arrives with the marker on its node - you are here - and what it adds. |
| | | 23 | Team arrives; Local's node fills as passed. |
| | | 24 | Global arrives. |
| | | 25 | The details give way and the invariant rises under the stage names: "Programming model ILattice unchanged at every stage"; the three lines of code that stay the same fade in beneath it, from the companion page. |
| Three ways in | `ways` | 26 | "Three ways in": three nodes on a chain; beneath, the mark, the name and the site's address in link blue. |
| | | 27 | Build, with the marker, and who it is for. |
| | | 28 | Evaluate; Build fills as passed. |
| | | 29 | Operate; the frame holds through the closing pause, then fades. |
