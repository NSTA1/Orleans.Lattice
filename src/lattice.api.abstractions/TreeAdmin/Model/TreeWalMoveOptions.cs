namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Optional tunables for the tree-admin WAL move execute verb. Every field is
/// zero-defaulted so an omitted value takes the core's conventional default (a
/// 30-second quiesce lease, 256-entry copy pages, and verify-after-copy enabled).
/// The control-API mirror of the core WAL move options, exposing the
/// single-partition tunables (the batch-only concurrency knob is not surfaced,
/// since the facade moves one partition at a time).
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeWalMoveOptions)]
[Immutable]
public readonly record struct TreeWalMoveOptions
{
    /// <summary>
    /// How long, in seconds, the source partition stays fenced while the move
    /// copies its tail and flips the placement pin. A value of zero (the default)
    /// takes the core's conventional 30-second lease.
    /// </summary>
    [Id(0)] public double QuiesceLeaseSeconds { get; init; }

    /// <summary>
    /// The number of entries copied per page from source to target. A value of
    /// zero or negative (the default) takes the core's conventional 256-entry page.
    /// </summary>
    [Id(1)] public int CopyPageSize { get; init; }

    /// <summary>
    /// When <see langword="false"/> (the default), the move verifies the target
    /// tail matches the copied source range before flipping the placement pin. Set
    /// <see langword="true"/> to skip that verification.
    /// </summary>
    [Id(2)] public bool DisableVerifyAfterCopy { get; init; }
}
