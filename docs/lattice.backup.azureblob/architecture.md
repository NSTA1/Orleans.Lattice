# Orleans.Lattice.Backup.AzureBlob architecture

This page describes how the Azure Blob Storage sink attaches to the backup engine and how it lays out and commits blobs. The sink implementation is internal; it is described here by behaviour and reached only through the core `ILatticeBackupSink` seam.

## Where it attaches

The backup engine (capture, incremental, restore, retention, and the control facade) reads and writes backups exclusively through the `ILatticeBackupSink` interface. `AddLatticeBackupAzureBlob` replaces whichever sink is registered - normally the in-cluster default installed by `AddLatticeBackup` - with the Azure Blob implementation. Because the seam is the only coupling point, no engine code changes: the same content-addressed artifacts and self-describing manifests flow, now landing in blob storage.

The sink is constructed once from the resolved `LatticeBackupAzureBlobOptions`: the options are validated, a long-lived `BlobContainerClient` is built for the configured account and container, and the container is created on first use (idempotent) so the host need not provision it. When a pre-built `BlobServiceClient` is supplied it is used verbatim and the host owns its lifetime and client options; otherwise the client is built from the connection string or the service URI plus credential, with any host-supplied client-options callback applied.

## Blob layout

Manifests and artifacts live under two distinct, lexicographically ordered prefixes:

- `manifests/{backupId}` - one **block blob** per manifest, keyed by backup id.
- `artifacts/{artifactId}` - one **append blob** per content-addressed artifact.

Azure Blob Storage returns listings in lexicographical name order, and the ids never contain a `/`, so listing a prefix yields ids in id order - exactly the ordering the `ILatticeBackupSink` contract requires of its manifest and artifact enumerations. Reading or listing a chain is therefore a single ordered prefix scan.

## Streaming artifacts

The sink's artifact surface is chunk-streaming on both write and read, matching the seam contract, so a large tree is captured and restored without buffering the payload whole. On write, the ordered chunk stream is appended to the artifact's append blob chunk by chunk. On read, the blob is streamed back as an ordered chunk sequence. Artifact ids are content-addressed (lowercase hex SHA-256), so an identical artifact resolves to the same blob name and a retry does not duplicate content.

## The append-blob commit protocol

An append blob is created before its chunks are fully written, so a crash mid-write could otherwise leave a partial blob indistinguishable from a complete one. The sink guards against this with a blob-metadata commit marker: once every chunk of an artifact has been appended, a `committed` metadata key is set to `true`. A partially-written blob therefore lacks the marker, so a retried write recognises it as incomplete and overwrites it rather than treating it as an idempotent no-op. A fully committed blob carries the marker and an identical retry is a genuine no-op.

## Manifest storage

Manifests are stored as block blobs under the `manifests/` prefix, serialized with the same Orleans manifest serializer the rest of the engine uses (the sink resolves it from DI at registration). Writing the same manifest id twice overwrites in place, so manifest writes are idempotent. Deleting a manifest removes only its block blob and does not touch the artifacts it references, matching the seam contract - artifact lifetime is governed by retention and deletion at the engine layer, which deletes only artifacts no retained manifest still references.

## Sharing an account

Because every blob sits under the configured container and prefix, multiple Lattice clusters can share one storage account without colliding by giving each its own `ContainerName`. The default container name is used when none is specified.
