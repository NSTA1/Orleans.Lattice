namespace Orleans.Lattice.Primitives;

/// <summary>
/// A delta representing changes to a leaf node since a given version.
/// Contains only the entries whose <see cref="LwwValue{T}.Timestamp"/> is
/// strictly newer than the corresponding entry in the requester's version vector.
/// </summary>
[GenerateSerializer]
[Immutable]
public sealed record StateDelta
{
    /// <summary>The changed entries: key → LWW-wrapped value (including tombstones).</summary>
    [Id(0)] public required Dictionary<string, LwwValue<byte[]>> Entries { get; init; }

    /// <summary>
    /// The version vector of the sender <em>at the time the delta was extracted</em>.
    /// The receiver should merge this into its own vector after applying the entries.
    /// </summary>
    [Id(1)] public required VersionVector Version { get; init; }

    /// <summary><c>true</c> if there were no changes to send.</summary>
    public bool IsEmpty => Entries.Count == 0;
}
