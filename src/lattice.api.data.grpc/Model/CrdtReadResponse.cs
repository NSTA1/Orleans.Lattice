namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire response for the unified typed-CRDT read RPC. Carries every logical read
/// shape as an optional field; the client reads only the field matching the
/// <see cref="CrdtReadRequest.Kind"/> it asked for and ignores the rest. An
/// absent or unreadable key yields the empty value for that kind (0, false, or an
/// empty collection).
/// </summary>
/// <remarks>
/// Not <c>[Immutable]</c>: it nests mutable value buffers materialised from the
/// decoded CRDT, so it must remain copy-eligible across the gRPC boundary.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.CrdtReadResponse)]
public sealed record CrdtReadResponse
{
    /// <summary>The converged total for a PN-counter read.</summary>
    [Id(0)] public long CounterValue { get; init; }

    /// <summary>The boolean state for an OR-Flag / RW-Flag read.</summary>
    [Id(1)] public bool FlagValue { get; init; }

    /// <summary>The element / value bytes for an OR-Set, MV-Register, or Sequence read.</summary>
    [Id(2)] public List<byte[]> Elements { get; init; } = [];

    /// <summary>The per-replica clocks for a version-vector read.</summary>
    [Id(3)] public List<CrdtVectorEntry> Vector { get; init; } = [];

    /// <summary>The per-field concurrent values for an OR-Map read.</summary>
    [Id(4)] public List<CrdtMapField> Map { get; init; } = [];

    /// <summary>
    /// Compares two responses by value, with <see cref="Elements"/> compared by
    /// byte content and <see cref="Vector"/> / <see cref="Map"/> compared
    /// element-by-element. The compiler-generated record equality compares each
    /// list with <see cref="EqualityComparer{T}.Default"/> (reference equality
    /// for the list, and for the <see cref="byte"/> arrays inside
    /// <see cref="Elements"/>), so two structurally identical responses - and,
    /// in particular, a response and its post-serialization self - would
    /// otherwise never compare equal.
    /// </summary>
    /// <param name="other">The response to compare against.</param>
    public bool Equals(CrdtReadResponse? other) =>
        other is not null
        && CounterValue == other.CounterValue
        && FlagValue == other.FlagValue
        && BytesListEqual(Elements, other.Elements)
        && ListEqual(Vector, other.Vector)
        && ListEqual(Map, other.Map);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(CounterValue);
        hash.Add(FlagValue);
        if (Elements is { } elements)
        {
            hash.Add(elements.Count);
            foreach (var element in elements)
            {
                if (element is { } bytes)
                {
                    hash.AddBytes(bytes);
                }
                else
                {
                    hash.Add(0);
                }
            }
        }

        AddListToHash(ref hash, Vector);
        AddListToHash(ref hash, Map);
        return hash.ToHashCode();
    }

    private static void AddListToHash<T>(ref HashCode hash, List<T>? items)
    {
        if (items is null)
        {
            return;
        }

        hash.Add(items.Count);
        foreach (var item in items)
        {
            hash.Add(item);
        }
    }

    private static bool BytesListEqual(List<byte[]>? left, List<byte[]>? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null || left.Count != right.Count)
        {
            return false;
        }

        for (var i = 0; i < left.Count; i++)
        {
            if (!BytesEqual(left[i], right[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool ListEqual<T>(List<T>? left, List<T>? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null || left.Count != right.Count)
        {
            return false;
        }

        for (var i = 0; i < left.Count; i++)
        {
            if (!EqualityComparer<T>.Default.Equals(left[i], right[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
