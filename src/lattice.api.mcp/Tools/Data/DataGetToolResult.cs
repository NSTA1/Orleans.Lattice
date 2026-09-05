namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the <c>data_get</c> point-read tool. <see cref="Found"/>
/// distinguishes a present value (including an empty one) from an absent key. A
/// key the caller may not read is reported as absent (<see cref="Found"/> is
/// <see langword="false"/>), never a value - the underlying facade prunes it
/// silently rather than throwing.
/// </summary>
public sealed record DataGetToolResult
{
    /// <summary>Logical tree the key was read from.</summary>
    public required string TreeId { get; init; }

    /// <summary>The key that was read.</summary>
    public required string Key { get; init; }

    /// <summary>
    /// <see langword="true"/> when a live value was returned; <see langword="false"/>
    /// when the key is absent, tombstoned, or hidden from the caller.
    /// </summary>
    public bool Found { get; init; }

    /// <summary>
    /// The value bytes when <see cref="Found"/> is <see langword="true"/>;
    /// otherwise an empty array. Base64-encoded in JSON structured content.
    /// </summary>
    public byte[] Value { get; init; } = Array.Empty<byte>();

    /// <summary>
    /// The per-key convergence mode of the value (e.g. <c>"PnCounter"</c>,
    /// <c>"OrSet"</c>), or <see langword="null"/> for a plain last-writer-wins
    /// value. Resolved from the leaf's own per-key discriminator, so it is reported
    /// even on a local, non-replicated, or mixed-mode tree. A non-<see langword="null"/>
    /// mode means <see cref="Value"/> holds the CRDT's internal serialized state
    /// (see <see cref="Raw"/>): use the matching typed getter (e.g.
    /// <c>data_pncounter_get</c>, <c>data_orset_get</c>) for the logical value, or
    /// the state API's <c>scan_entries</c> / <c>get_entry</c>, which decode members.
    /// </summary>
    public string? MergeMode { get; init; }

    /// <summary>
    /// <see langword="true"/> whenever <see cref="Found"/> is <see langword="true"/>:
    /// the data plane returns the raw stored bytes verbatim and never decodes a typed
    /// CRDT into a logical projection, so a consumer must never mistake the internal
    /// serialization of a CRDT (indicated by a non-<see langword="null"/>
    /// <see cref="MergeMode"/>) for its logical value. Plain last-writer-wins bytes
    /// (<see cref="MergeMode"/> is <see langword="null"/>) are the application's own
    /// value and safe to use directly.
    /// </summary>
    public bool Raw { get; init; }

    /// <summary>
    /// Compares two results by value, with <see cref="Value"/> compared by
    /// content. The compiler-generated record equality compares the
    /// <see cref="byte"/> array with <see cref="EqualityComparer{T}.Default"/>
    /// (reference equality), so two structurally identical results - and, in
    /// particular, a result and its post-serialization self - would otherwise
    /// never compare equal.
    /// </summary>
    /// <param name="other">The result to compare against.</param>
    public bool Equals(DataGetToolResult? other) =>
        other is not null
        && string.Equals(TreeId, other.TreeId, StringComparison.Ordinal)
        && string.Equals(Key, other.Key, StringComparison.Ordinal)
        && Found == other.Found
        && string.Equals(MergeMode, other.MergeMode, StringComparison.Ordinal)
        && Raw == other.Raw
        && BytesEqual(Value, other.Value);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(TreeId, StringComparer.Ordinal);
        hash.Add(Key, StringComparer.Ordinal);
        hash.Add(Found);
        hash.Add(MergeMode, StringComparer.Ordinal);
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
