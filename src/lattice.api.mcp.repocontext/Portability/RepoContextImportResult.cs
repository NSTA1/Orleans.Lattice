namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The outcome of importing a repository-context snapshot: how many records were
/// read and how they landed, plus the format version of the imported stream.
/// </summary>
internal readonly record struct RepoContextImportResult
{
    /// <summary>The format version stamped in the imported stream's header.</summary>
    public int FormatVersion { get; init; }

    /// <summary>The total number of records read from the snapshot.</summary>
    public long RecordsRead { get; init; }

    /// <summary>
    /// The number of imported records whose key was already present in the target
    /// store and was therefore CRDT-merged rather than written for the first time.
    /// </summary>
    public long RecordsMerged { get; init; }

    /// <summary>The number of imported records whose vector payload was applied.</summary>
    public long VectorsApplied { get; init; }
}
