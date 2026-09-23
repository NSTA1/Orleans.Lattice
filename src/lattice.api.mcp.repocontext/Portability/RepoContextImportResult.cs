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

    /// <summary>
    /// The number of records the import declined to write because their captured
    /// expiry had already elapsed by the time the snapshot was restored. Such a
    /// record describes an entry the source store was already entitled to shed, so
    /// writing it would resurrect it; the record is dropped instead, along with any
    /// vector payload it carried.
    /// </summary>
    public long RecordsExpired { get; init; }

    /// <summary>
    /// The number of records the import actually wrote to the target store:
    /// <see cref="RecordsRead"/> less <see cref="RecordsExpired"/>. Callers that
    /// verify a restore landed completely must count against this rather than
    /// <see cref="RecordsRead"/>, because a dropped expired record is a correct
    /// outcome and not a shortfall.
    /// </summary>
    public long RecordsWritten => RecordsRead - RecordsExpired;
}
