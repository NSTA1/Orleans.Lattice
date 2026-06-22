namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// Reads entries for the Data tab over the public state-API entry surface.
/// Trees and views are addressed uniformly by id; no grain access.
/// </summary>
public interface IDataReader
{
    /// <summary>
    /// Scans a page of entries for <paramref name="treeId"/>. Pass the
    /// <paramref name="continuationToken"/> from a prior page to resume the same
    /// snapshot, or <see langword="null"/> to open a fresh snapshot scan.
    /// </summary>
    Task<DataPage> ScanAsync(
        string treeId,
        int pageSize,
        string? continuationToken = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Fetches the full record for a single key, or <see langword="null"/> when
    /// the key is absent.
    /// </summary>
    Task<DataEntry?> GetEntryAsync(string treeId, string key, CancellationToken cancellationToken = default);
}
