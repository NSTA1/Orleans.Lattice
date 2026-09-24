# Orleans.Lattice.Backup.AzureBlob

Optional, opt-in **Azure Blob Storage** backup sink for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice).

Implements the `ILatticeBackupSink` seam from `Orleans.Lattice.Backup` against an
Azure Storage account so backups are persisted durably off-cluster:

- Backup **artifacts** are stored as **append blobs** under an
  `artifacts/` prefix, so a streamed capture appends in natural order and reads
  back in order.
- Self-describing **manifests** are stored as **block blobs** under a
  `manifests/` prefix, keyed by backup id, so listing a chain is efficient and
  ordered.
- Writes are **idempotent** per artifact id: a blob-metadata commit marker makes
  a retried write against a committed blob a no-op, while a partially-written
  blob is recreated and rewritten.

The sink is selected purely by DI - core and the capture engine stay unaware of
Azure specifics. Register it on a silo:

```csharp
siloBuilder
    .AddLattice(/* ... */)
    .AddLatticeBackup()
    .AddLatticeBackupAzureBlob(options =>
    {
        options.ConnectionString = "UseDevelopmentStorage=true";
        options.ContainerName = "orleans-lattice-backup";
    });
```

The last registration wins, so this call replaces the default in-cluster sink.
