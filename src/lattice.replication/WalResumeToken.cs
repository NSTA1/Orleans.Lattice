namespace Orleans.Lattice.Replication;

/// <summary>
/// Internal opaque resume token shape for transport-side seams that need to
/// resume from a precise per-WAL-partition offset rather than a
/// hybrid-logical-clock cursor.
/// <para>
/// <b>Cursor-shape decision.</b> The public
/// <see cref="IChangeFeed"/> still keeps an HLC overload for source
/// compatibility, but the HLC cursor is not applied there; the
/// <see cref="ChangeFeedCursor"/> overload is the public offset-based
/// resume shape. This token remains the internal transport-side
/// equivalent for seams that persist WAL partition positions directly.
/// </para>
/// <para>
/// Per-partition offsets are exposed on the internal transport-side
/// seam where they trivially are monotonic per partition, match the WAL
/// <see cref="WalEntry.Offset"/> shape 1:1, and remove HLC-skew edge
/// cases at reconnect time. Receivers store this token alongside their
/// per-origin HWM as resume/progress state; the HWM is not an
/// incremental-write dedup threshold.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.WalResumeToken)]
[Immutable]
internal readonly record struct WalResumeToken
{
    /// <summary>The per-tree WAL partition index this token resumes against.</summary>
    [Id(0)] public int ShardIndex { get; init; }

    /// <summary>
    /// Inclusive lower-bound offset to resume from. The next emitted
    /// entry will have <see cref="WalEntry.Offset"/> equal to
    /// <see cref="Offset"/> + 1.
    /// </summary>
    [Id(1)] public long Offset { get; init; }
}
