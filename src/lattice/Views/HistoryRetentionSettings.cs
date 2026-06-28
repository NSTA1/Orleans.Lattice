namespace Orleans.Lattice;

/// <summary>
/// The effective per-tree durable-history retention policy: the
/// <see cref="HistoryRetentionMode"/> applied to LWW value bytes and the
/// age-bound <see cref="Window"/> after which a revision row is reaped by the
/// normal entry-expiry path. Returned by
/// <see cref="ILattice.GetHistoryRetentionAsync(System.Threading.CancellationToken)"/>.
/// <para>
/// A <see cref="Window"/> of <see cref="System.TimeSpan.Zero"/> means revisions
/// do not expire (storage is bounded only by the source mutation rate, not by
/// age). The policy is a live-tunable override stored in the tree registry; a
/// change takes effect for revisions written after it and never rewrites or
/// rebuilds existing rows.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.HistoryRetentionSettings)]
[Immutable]
public readonly record struct HistoryRetentionSettings
{
    /// <summary>The retention mode for LWW value bytes.</summary>
    [Id(0)] public HistoryRetentionMode Mode { get; init; }

    /// <summary>
    /// The age after which a revision row expires, or
    /// <see cref="System.TimeSpan.Zero"/> for no age bound.
    /// </summary>
    [Id(1)] public TimeSpan Window { get; init; }
}
