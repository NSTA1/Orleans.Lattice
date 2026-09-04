namespace Orleans.Lattice.Api.Data;

/// <summary>
/// Result of a point read. <see cref="Found"/> distinguishes a present value
/// (including an empty one) from an absent key. A key the caller lacks read
/// permission for is reported as absent (<see cref="Found"/> is
/// <see langword="false"/>), never as a value - the gated
/// <see cref="ILattice"/> read path prunes it silently rather than throwing.
/// </summary>
[GenerateSerializer]
[Alias(DataApiTypeAliases.DataReadResult)]
public sealed record DataReadResult
{
    /// <summary>Logical tree the key was read from.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The key that was read.</summary>
    [Id(1)] public required string Key { get; init; }

    /// <summary>
    /// <see langword="true"/> when a live value was returned; <see langword="false"/>
    /// when the key is absent, tombstoned, or hidden from the caller by the
    /// access gate.
    /// </summary>
    [Id(2)] public bool Found { get; init; }

    /// <summary>
    /// The value bytes when <see cref="Found"/> is <see langword="true"/>;
    /// otherwise an empty array.
    /// </summary>
    [Id(3)] public byte[] Value { get; init; } = Array.Empty<byte>();

    /// <summary>
    /// The per-key convergence discriminator for the key, or <see langword="null"/>
    /// when the key is a plain last-writer-wins value (or the mode was not recorded).
    /// Resolved from the leaf's own per-key mode map, so it is reported even on a
    /// local, non-replicated, or mixed-mode tree. A non-<see langword="null"/> mode
    /// means <see cref="Value"/> holds the CRDT's internal serialized state (see
    /// <see cref="Raw"/>): use the matching typed getter (e.g. the PN-counter /
    /// OR-set read) for the logical value, or the read-only state API's
    /// <c>scan_entries</c> / <c>get_entry</c>, which decode members directly.
    /// </summary>
    [Id(4)] public LatticeMergeMode? MergeMode { get; init; }

    /// <summary>
    /// Always <see langword="true"/> when <see cref="Found"/> is
    /// <see langword="true"/>: the data plane returns the raw stored bytes verbatim
    /// and never decodes a typed CRDT into a logical projection. Surfaced so a
    /// consumer never mistakes a CRDT's internal serialization (indicated by a
    /// non-<see langword="null"/> <see cref="MergeMode"/>) for its logical value.
    /// Plain last-writer-wins bytes (<see cref="MergeMode"/> is
    /// <see langword="null"/>) are the application's own value and safe to use.
    /// </summary>
    [Id(5)] public bool Raw { get; init; }

    /// <summary>
    /// Compares two results by value, with <see cref="Value"/> compared by
    /// content. The compiler-generated record equality compares the
    /// <see cref="byte"/> array with <see cref="EqualityComparer{T}.Default"/>
    /// (reference equality), so two structurally identical results - and, in
    /// particular, a result and its post-serialization self - would otherwise
    /// never compare equal.
    /// </summary>
    /// <param name="other">The result to compare against.</param>
    public bool Equals(DataReadResult? other) =>
        other is not null
        && string.Equals(TreeId, other.TreeId, StringComparison.Ordinal)
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && Found == other.Found
        && MergeMode == other.MergeMode
        && Raw == other.Raw
        && BytesEqual(Value, other.Value);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(TreeId, StringComparer.Ordinal);
        hash.Add(Key, StringComparer.Ordinal);
        hash.Add(Found);
        hash.Add(MergeMode);
        hash.Add(Raw);
        if (Value is { } value)
        {
            hash.AddBytes(value);
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
