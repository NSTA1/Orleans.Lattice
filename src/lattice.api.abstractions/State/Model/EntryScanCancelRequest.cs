namespace Orleans.Lattice.Api.State;

/// <summary>
/// Request for the entry-scan cancel endpoint
/// (<see cref="ILatticeStateQuery.CancelScanAsync"/>). Releases the
/// server-side snapshot cursor named by a continuation token so its
/// WAL-retention pin and per-shard baseline are freed promptly instead of
/// lingering until the cursor's idle TTL. Cancelling an unknown, already-drained,
/// or already-closed cursor is a no-op.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.EntryScanCancelRequest)]
[Immutable]
public sealed record EntryScanCancelRequest
{
    /// <summary>Logical tree identifier the cursor was opened against.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// The continuation token of the cursor to release, as returned by a prior
    /// <see cref="ILatticeStateQuery.ScanEntriesAsync"/> page. A <see langword="null"/>
    /// or empty token is a no-op.
    /// </summary>
    [Id(1)] public string? ContinuationToken { get; init; }
}
