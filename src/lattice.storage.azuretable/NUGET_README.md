# Orleans.Lattice.Storage.AzureTable

Durable Azure Table Storage backend for the Orleans.Lattice write-ahead log (IWalStorageProvider). Pluggable via the core AddWalStorage(...) seam, so single-cluster deployments under the WAL-as-sole-commit-point flip can persist their commit log without taking a hard reference on the replication package.

See docs/lattice.storage.azuretable/README.md for the full storage layout, transactional batch contract, capacity planning, and operations guide. For the core WAL provider seam, see docs/lattice/wal-storage-providers.md.
