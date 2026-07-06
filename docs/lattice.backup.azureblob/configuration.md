# Orleans.Lattice.Backup.AzureBlob configuration

The package has a single public options type, `LatticeBackupAzureBlobOptions`, bound through `AddLatticeBackupAzureBlob(configure)`.

## `LatticeBackupAzureBlobOptions`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `ConnectionString` | `string?` | `null` | Storage-account connection string. When set, the sink builds the service client from it. |
| `ServiceUri` | `Uri?` | `null` | Storage-account blob-service endpoint URI (for example `https://{account}.blob.core.windows.net`). When set, one of `TokenCredential` or `SharedKeyCredential` must also be supplied. |
| `TokenCredential` | `Azure.Core.TokenCredential?` | `null` | Azure AD credential used with `ServiceUri`. Pair with `new DefaultAzureCredential()` for managed-identity scenarios. Mutually exclusive with `SharedKeyCredential`. |
| `SharedKeyCredential` | `Azure.Storage.StorageSharedKeyCredential?` | `null` | Shared-key credential used with `ServiceUri`. Mutually exclusive with `TokenCredential`. |
| `ServiceClient` | `Azure.Storage.Blobs.BlobServiceClient?` | `null` | A pre-built service client used verbatim. When set, `ConfigureClientOptions` is ignored and the host owns the client's lifetime and options. Mutually exclusive with the connection-string and service-URI modes. |
| `ContainerName` | `string` | `DefaultContainerName` (`orleans-lattice-backup`) | The blob container that backs the sink. Created on first use (idempotent). Specify a non-default name to share an account across multiple clusters without collisions. |
| `ConfigureClientOptions` | `Action<Azure.Storage.Blobs.BlobClientOptions>?` | `null` | Optional callback invoked when the sink builds the client options, to attach custom retry policies, diagnostics, or transport. Ignored when `ServiceClient` is supplied. |

### Constant

`const string DefaultContainerName = "orleans-lattice-backup"` - the default `ContainerName`. It is lowercase alphanumeric with hyphens and within the three-to-sixty-three-character range Azure Blob Storage requires of a container name.

## Authentication-mode rules

Exactly one authentication mode must be configured. The options are validated when the sink is first resolved; a violation throws `InvalidOperationException` with an actionable message:

- **Exactly one of** `ConnectionString`, `ServiceUri`, or `ServiceClient` must be set. Zero modes, or more than one, is rejected.
- When `ServiceUri` is set, **exactly one** of `TokenCredential` or `SharedKeyCredential` must accompany it.
- A credential (`TokenCredential` or `SharedKeyCredential`) without `ServiceUri` is rejected.
- `ContainerName` must not be null or whitespace.

The authentication mode is read once at construction; subsequent edits to these fields are not observed by the already-built container client.
