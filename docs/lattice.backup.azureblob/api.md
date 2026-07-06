# Orleans.Lattice.Backup.AzureBlob API reference

The package has two public types: the options record and the registration extension. The sink implementation itself is internal and is reached only through the core `ILatticeBackupSink` seam; its behaviour is described in [Architecture](architecture.md).

## `LatticeBackupAzureBlobServiceCollectionExtensions`

Static extension method on `ISiloBuilder`.

- `ISiloBuilder AddLatticeBackupAzureBlob(this ISiloBuilder builder, Action<LatticeBackupAzureBlobOptions> configure)`

  Registers the Azure Blob Storage sink as the silo's `ILatticeBackupSink`, replacing the in-cluster default that `AddLatticeBackup` installs. The registration is idempotent (a repeat call keeps the last configuration) and may be called before or after `AddLatticeBackup` because it replaces the sink registration outright. The container client is built once, from the populated authentication mode, when the sink is first resolved. Throws `ArgumentNullException` when `builder` or `configure` is null.

## `LatticeBackupAzureBlobOptions`

Configuration for the Azure Blob Storage sink. Exactly one authentication mode must be configured; see [Configuration](configuration.md) for the full property table, defaults, and mode rules. In summary:

- `const string DefaultContainerName = "orleans-lattice-backup"` - the default container name.
- `string? ConnectionString` - storage-account connection string.
- `Uri? ServiceUri` - blob-service endpoint URI, paired with one credential below.
- `Azure.Core.TokenCredential? TokenCredential` - Azure AD credential used with `ServiceUri`.
- `Azure.Storage.StorageSharedKeyCredential? SharedKeyCredential` - shared-key credential used with `ServiceUri`.
- `Azure.Storage.Blobs.BlobServiceClient? ServiceClient` - a pre-built service client used verbatim.
- `string ContainerName` - the container that backs the sink (default `DefaultContainerName`).
- `Action<Azure.Storage.Blobs.BlobClientOptions>? ConfigureClientOptions` - optional callback to customise the client options the sink builds (ignored when `ServiceClient` is supplied).

The options type also carries internal helpers (`Validate`, `BuildContainerClient`) that the registration invokes; these are not part of the public surface.
