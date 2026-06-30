# Orleans.Lattice.Storage.AzureTable

Durable Azure Table Storage backend for the [Orleans.Lattice](https://www.nuget.org/packages/Orleans.Lattice) write-ahead log (`IWalStorageProvider`). Pluggable via the core `AddWalStorage(...)` seam, so single-cluster deployments can persist their commit log without taking a hard reference on the replication package.

See [`docs/lattice.storage.azuretable/README.md`](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.storage.azuretable/README.md) for the full storage layout, transactional batch contract, capacity planning, and operations guide. For the core WAL provider seam, see [`docs/lattice/wal-storage-providers.md`](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice/wal-storage-providers.md).
