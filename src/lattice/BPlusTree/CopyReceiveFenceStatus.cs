namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The receive-fence status of one physical tree copy (issue #4593), as
/// <see cref="ICopyReceiveFenceGrain.GetStatusAsync"/> reports it.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CopyReceiveFenceStatus)]
[Immutable]
internal readonly record struct CopyReceiveFenceStatus
{
    /// <summary>Whether a coordinated restore still holds the copy closed.</summary>
    [Id(0)] public bool Closed { get; init; }

    /// <summary>
    /// The lowest receive-fence epoch an apply to this copy may have been admitted
    /// under: the epoch of the pause the restore took before the copy became
    /// routable. Zero for a copy no restore ever closed.
    /// </summary>
    [Id(1)] public long MinAdmissionEpoch { get; init; }
}
