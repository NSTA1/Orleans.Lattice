namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Durable state of <see cref="ICopyReceiveFenceGrain"/>: the restore saga that
/// closed the copy, when, and the copy's minimum admission epoch.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CopyReceiveFenceState)]
internal sealed class CopyReceiveFenceState
{
    /// <summary>The saga that closed the copy, or <see langword="null"/> when open.</summary>
    [Id(0)]
    public string? ClosedBySagaId { get; set; }

    /// <summary>UTC ticks at which the copy was closed; zero when open.</summary>
    [Id(1)]
    public long ClosedAtTicks { get; set; }

    /// <summary>
    /// The lowest receive-fence epoch an apply to this copy may have been
    /// admitted under. Kept after the copy opens; zero for a copy no restore
    /// ever closed.
    /// </summary>
    [Id(2)]
    public long MinAdmissionEpoch { get; set; }
}
