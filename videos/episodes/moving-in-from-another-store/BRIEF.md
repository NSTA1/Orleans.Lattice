# Moving in from another store - brief

- **Path:** Build, episode 7, after Going durable.
- **Audience:** developers moving an existing dataset into a Lattice tree.
- **The one idea:** a safe migration is a planned one-way bulk load into a new
  tree, followed by verification and the source changes since the chosen cut.
- **Introduces:** External store migration
  ([the playbook](../../../docs/lattice/external-store-migration.md)).
- **Afterwards the viewer can** choose an ingest path for the dataset, prepare
  keys and values for the tree, and explain why verification and a delta pass
  come before moving readers.

## Beats

1. Recap the durable tree from Going durable, then set the context: one-way
   imports from Redis, a relational database or Cosmos DB.
2. Before writing, decide how to ingest, how to compose keys and how to encode
   values.
3. Choose between the in-memory list, an ordered in-cluster stream and the
   acknowledged tree-administration session.
4. Make the key order explicit: ordinal comparison and fixed-width numeric
   segments; values are bytes and their encoding is the migrator's decision.
5. Load a consistent source cut into a fresh tree, compare counts and spot-check
   boundaries, apply the delta, then move readers.
6. End the Build path at the documentation site's Samples page.

## Sources

- [External store migration](../../../docs/lattice/external-store-migration.md):
  ingest paths, key ordering, value encoding, source-store playbooks, validation
  and cutover.
- [Production order](../../series.md#production-order): B7 follows Going durable
  and hands off to the Samples page.

## Not in this episode

- A live replication or rollback mechanism; bulk load is a one-way import.
- A source-specific implementation or a complete migration runbook.
- Schema evolution, TTL restoration, storage limits and replication behavior
  beyond noting that values are opaque bytes.
