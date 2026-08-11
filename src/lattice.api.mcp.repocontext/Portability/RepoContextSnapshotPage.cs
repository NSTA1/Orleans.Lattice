namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One page of a resumable namespace enumeration over the repository-context
/// store, produced by
/// <see cref="RepoContextPortability.EnumerateAsync"/>. Carries the records in
/// this page (in ascending key order), a <see cref="ContinuationToken"/> to
/// resume after the last record, and whether the underlying scan has more pages.
/// </summary>
internal sealed record RepoContextSnapshotPage
{
    /// <summary>The records in this page, in ascending key order.</summary>
    public required IReadOnlyList<RepoContextSnapshotRecord> Records { get; init; }

    /// <summary>
    /// An opaque token to pass back to
    /// <see cref="RepoContextPortability.EnumerateAsync"/> to resume enumeration
    /// after the last record in this page, or <see langword="null"/> when the scan
    /// is exhausted. Resuming with this token never re-yields a record already
    /// returned in an earlier page.
    /// </summary>
    public string? ContinuationToken { get; init; }

    /// <summary>
    /// <see langword="true"/> when the scan may have more records beyond this
    /// page (and <see cref="ContinuationToken"/> is non-null);
    /// <see langword="false"/> once the scan is drained.
    /// </summary>
    public required bool HasMore { get; init; }
}
