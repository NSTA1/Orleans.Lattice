# Orleans.Lattice.Backup.AzureBlob architecture

This page describes how the Azure Blob Storage sink attaches to the backup engine and how it lays out and commits blobs. The sink implementation is internal; it is described here by behaviour and reached only through the core `ILatticeBackupSink` seam.

## Where it attaches

The backup engine (capture, incremental, restore, retention, and the control facade) reads and writes backups exclusively through the `ILatticeBackupSink` interface. `AddLatticeBackupAzureBlob` replaces whichever sink is registered - normally the in-cluster default installed by `AddLatticeBackup` - with the Azure Blob implementation. Because the seam is the only coupling point, no engine code changes: the same per-capture artifacts and self-describing, content-addressed manifests flow, now landing in blob storage. The sink reports `IsDurable` as `true`, so the engine's durable-sink-gated features - notably the periodic backup-health monitor - are active against it.

The sink is constructed once from the resolved `LatticeBackupAzureBlobOptions`: the options are validated, a long-lived `BlobContainerClient` is built for the configured account and container, and the container is created on first use (idempotent) so the host need not provision it. When a pre-built `BlobServiceClient` is supplied it is used verbatim and the host owns its lifetime and client options; otherwise the client is built from the connection string or the service URI plus credential, with any host-supplied client-options callback applied.

## Blob layout

Manifests and artifacts live under two distinct, lexicographically ordered prefixes:

- `manifests/{backupId}` - one **block blob** per manifest, keyed by backup id.
- `artifacts/{artifactId}` - one **append blob** per artifact.

Azure Blob Storage returns listings in lexicographical name order, so listing a prefix yields ids in id order - exactly the ordering the `ILatticeBackupSink` contract requires of its manifest and artifact enumerations. Reading or listing a chain is therefore a single ordered prefix scan.

An id may contain a `/` - a tenant-composed tree id of the form `t/{tenant}/{name}` is embedded verbatim in every artifact id - so the separator itself is permitted. What the sink rejects, before concatenating an id onto its prefix, is any id whose resolved blob address would escape that prefix: an id that starts with `/`, contains a backslash or a control character, or has an empty, `.` or `..` segment. The check runs on the id as written and again after one percent-decode, because the Azure SDK resolves blob addresses through `Uri`, which removes dot segments after decoding.

## Streaming artifacts

The sink's artifact surface is chunk-streaming on both write and read, matching the seam contract, so a large tree is captured and restored without buffering the payload whole. On write, the ordered chunk stream is appended to the artifact's append blob chunk by chunk, each chunk framed with a 4-byte little-endian length prefix (a framed chunk larger than the 4 MiB append-block limit is split across several append blocks, which does not change the frame). On read, the blob is streamed back frame by frame, so the reader receives exactly the chunk boundaries that were written; a frame whose declared length is negative or larger than the blob is rejected as corrupt. The capture engine names each artifact with a per-capture id and records the artifact's SHA-256 digest in the manifest rather than in the blob name, so a retried write of the same artifact id lands on the same blob and the commit protocol below decides whether it is a no-op.

## The append-blob commit protocol

An append blob is created before its chunks are fully written, so a crash mid-write could otherwise leave a partial blob indistinguishable from a complete one. The sink guards against this with a blob-metadata commit marker: once every chunk of an artifact has been appended, a `committed` metadata key is set to `true`. A partially-written blob therefore lacks the marker, so a retried write recognises it as incomplete and overwrites it rather than treating it as an idempotent no-op. A fully committed blob carries the marker and an identical retry is a genuine no-op. The marker also governs discovery: the artifact listing omits an uncommitted blob, and the resolvability probe used by reconcile, scrub, and health checks reports it as missing.

## Manifest storage

Manifests are stored as block blobs under the `manifests/` prefix, serialized with the same Orleans manifest serializer the rest of the engine uses (the sink resolves it from DI when it is first resolved). Writing the same manifest id twice overwrites in place, so manifest writes are idempotent. Deleting a manifest removes only its block blob and does not touch the artifacts it references, matching the seam contract - artifact lifetime is governed by retention and deletion at the engine layer, which deletes only artifacts no retained manifest still references.

## Sharing an account

Because every blob sits under the configured container and prefix, multiple Lattice clusters can share one storage account without colliding by giving each its own `ContainerName`. The default container name is used when none is specified. Clusters in one replication set are the exception: a coordinated restore of a replicated tree resolves the manifest chain from every cluster's own sink, so those clusters must point at the same container (the backup package's cross-cluster sink-sharing guard checks this; see [Cross-cluster sink sharing](../lattice.backup/configuration.md#cross-cluster-sink-sharing)).
