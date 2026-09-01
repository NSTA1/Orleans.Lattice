namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The complete set of index entries one grain currently contributes to an
/// index, together with the grain key they all point back to.
/// </summary>
/// <remarks>
/// <para>
/// A projection is the input to the next projection's diff: an enrolment hook
/// persists the projection it last wrote alongside the grain's own state, and
/// hands it back on the next mutation so
/// <see cref="GrainIndexUpdatePlan.Between(GrainIndexProjection, GrainIndexProjection)"/>
/// can work out which entries moved. That is why it is serializable, and why
/// it carries the full payload bytes rather than the keys alone - a property
/// whose type has no order-preserving encoding keeps a stable key and moves
/// only its payload, so keys alone would miss the change.
/// </para>
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexProjection)]
public sealed class GrainIndexProjection
{
    private static readonly GrainIndexEntry[] NoEntries = [];

    /// <summary>Initialises a projection.</summary>
    /// <param name="grainKey">The encoded grain key every entry points back to. Must not be <c>null</c>.</param>
    /// <param name="entries">The entries, in projected-property declaration order. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexProjection(string grainKey, IReadOnlyList<GrainIndexEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(grainKey);
        ArgumentNullException.ThrowIfNull(entries);
        GrainKey = grainKey;
        Entries = entries;
    }

    /// <summary>The encoded grain key every entry in this projection points back to.</summary>
    [Id(0)] public string GrainKey { get; }

    /// <summary>
    /// The entries, in the order the index declared its projected properties.
    /// Two projections of the same definition are therefore index-aligned,
    /// which is what lets the diff walk them in one pass.
    /// </summary>
    [Id(1)] public IReadOnlyList<GrainIndexEntry> Entries { get; }

    /// <summary>
    /// An empty projection for <paramref name="grainKey"/> - the correct
    /// "previous" value for a grain that has never been indexed, and the result
    /// of projecting a grain whose index declares no properties.
    /// </summary>
    /// <param name="grainKey">The encoded grain key. Must not be <c>null</c>.</param>
    /// <returns>A projection with no entries.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="grainKey"/> is <c>null</c>.</exception>
    public static GrainIndexProjection Empty(string grainKey) => new(grainKey, NoEntries);
}
