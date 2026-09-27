# Hello, Lattice - storyboard

What is on screen for each cue of [SCRIPT.md](SCRIPT.md). The opening is the
shared `title-card`; every other scene is the code it talks about, laid out in
the composition itself, because the panels are this episode's own snippets
(copied from the companion page by `npm run snippets`, compiled with the rest
of the documentation). A bar in the marker beside a panel is "you are here":
it sits beside the lines the narration is on, and moves on each cue's beat,
stamped from the narration by `npm run timeline`.

| Scene | Component | Cue | On screen |
| --- | --- | --- | --- |
| Hello, Lattice | `title-card` | 1 | The mark and "Build"; "Hello, Lattice" as the title; "Register, resolve, write and read". |
| Register on a silo | composition | 2 | "Register on a silo". "The package, from NuGet", and beneath it `dotnet add package Orleans.Lattice` in mono. |
| | | 3 | The registration panel (`hello-lattice/register`) rises in; the marker beside `AddLattice` and its storage callback. |
| | | 4 | The marker moves to the in-memory lines: `AddMemoryGrainStorage` and the comment that the log is in memory by default. |
| | | 5 | The marker moves to "In production, make both storage surfaces durable." |
| A tree, by name | composition | 6 | "A tree, by name". The resolve panel (`hello-lattice/resolve`): `GetGrain<ILattice>("my-tree")`, on a client or inside a grain. |
| | | 7 | Beneath it, a filled node and "The same name, the same tree". |
| Typed values | composition | 8 | "Typed values". The typed panel (`hello-lattice/typed`); the marker beside "The typed overloads default to JSON". |
| | | 9 | The marker moves to the typed `SetAsync` and `GetAsync<User>`. |
| | | 10 | The marker moves to the raw `byte[]` write. |
| When a key is not there | composition | 11 | "When a key is not there". The absent-key panel (`hello-lattice/absent`); the marker beside the `GetAsync` of an absent key. |
| | | 12 | The marker moves to `DeleteAsync`. |
| | | 13 | The marker moves to `GetOrSetAsync`. |
| | | 14 | The marker goes; beside the panel, "A scan, in order": the live keys `hello` and `user/42`, sorted. |
| Where next | composition | 15 | "Where next". A filled node: "Values that merge", "Next on Build". |
| | | 16 | Two hollow nodes beneath: "Quick start", "The setup and the typed code"; "API reference", "The contract for each operation". |
