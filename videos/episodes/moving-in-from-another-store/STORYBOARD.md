# Moving in from another store - storyboard

What is on screen for each cue of [SCRIPT.md](SCRIPT.md). The episode uses the
shared title card, a variable-driven migration flow, and the shared choice list
for its ending. Its code panel is copied from the compiled snippet on the
companion page. Clip windows and beats are stamped from the narration by
`npm run timeline`.

| Scene | Component | Cue | On screen |
| --- | --- | --- | --- |
| Moving in from another store | `title-card` | 1-2 | Build / B7 title card, with the subtitle "Plan the import. Verify it. Then cut over." |
| Decide before loading | Composition scene | 3 | Three decisions: ingest path, key composition and value encoding. |
| Choose an ingest path | Composition scene | 4-5 | Three paths: list-based load, in-cluster ordered stream, and resumable administration session. |
| Move, then verify | `migration-flow` | 6-9 | Five steps: choose the ingest path, prepare ordinal keys and byte values, load a consistent source cut, verify count and ranges, then apply the delta and move readers. The marker moves with the narration. |
| Move, then verify | Code panel | 7-9 | The compiled list-based `BulkLoadAsync` call, shown from loading through verification and cutover. |
| Where next | `choice-list` | 10 | The Samples page, next after the Build path, and the External store migration playbook. |
