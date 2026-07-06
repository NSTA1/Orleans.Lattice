# Orleans.Lattice.Backup.AzureBlob

A durable [Azure Blob Storage](https://learn.microsoft.com/azure/storage/blobs/) sink for [Orleans.Lattice.Backup](../lattice.backup/README.md).

## What is it?

`Orleans.Lattice.Backup.AzureBlob` implements the backup engine's `ILatticeBackupSink` seam against an Azure Blob Storage container, so captured backups and their manifests live in durable external storage rather than in the default in-cluster tree. Registering it replaces the in-cluster default sink outright; because the capture and restore engines talk only to the `ILatticeBackupSink` seam, they stay entirely unaware of Azure specifics.

Backups are laid out under two deterministic prefixes in the container - manifests as block blobs, content-addressed artifacts as append blobs - so listing or reading a chain is a single ordered prefix scan.

## Core properties

- **Seam replacement, not a new API.** The package adds one options type and one registration extension; it exposes no bespoke backup surface. Everything is driven through the core `ILatticeBackupSink` contract.
- **Flexible authentication.** Exactly one of a connection string, a service URI with an Azure AD token credential, a service URI with a shared-key credential, or a pre-built `BlobServiceClient` is configured. The authentication mode is read once at construction and a long-lived container client is built from it.
- **Content-addressed and idempotent.** Artifacts are stored under their content-addressed id; a blob-metadata commit marker distinguishes a fully-written artifact from a partially-written one, so a retried write overwrites an incomplete blob rather than treating it as a done no-op.
- **Provisionless.** The container is created on first use (idempotent), so a host does not have to provision it out of band.

## Setup

Register the sink on the silo. It may be called before or after `AddLatticeBackup` because it replaces the sink registration outright.

```csharp
siloBuilder
    .AddLattice(/* core tree configuration */)
    .AddLatticeBackup()
    .AddLatticeBackupAzureBlob(options =>
    {
        options.ServiceUri = new Uri("https://myaccount.blob.core.windows.net");
        options.TokenCredential = new DefaultAzureCredential();
        options.ContainerName = "orleans-lattice-backup";
    });
```

## Reference

- [API reference](api.md) - the public options type and registration extension.
- [Configuration](configuration.md) - every public options property, its type, and its default, and the authentication-mode rules.
- [Architecture](architecture.md) - the blob layout, the append-blob commit protocol, and how the sink attaches to the core engine.

## See also

- [`Orleans.Lattice.Backup`](../lattice.backup/README.md) - the backup engine and the `ILatticeBackupSink` seam this package implements.
