using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Result returned by <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.GetOrSetAsync"/> containing
/// the existing value (if the key was already live) and an optional
/// <see cref="Orleans.Lattice.BPlusTree.SplitResult"/> when a write triggered a leaf split.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.GetOrSetResult)]
internal sealed record GetOrSetResult
{
    /// <summary>
    /// The existing value if the key was already live, or <c>null</c> if the
    /// value was newly written.
    /// </summary>
    [Id(0)] public byte[]? ExistingValue { get; init; }

    /// <summary>
    /// A split result if the write caused the leaf to split, otherwise <c>null</c>.
    /// Always <c>null</c> when <see cref="ExistingValue"/> is not <c>null</c>
    /// (no write occurred).
    /// </summary>
    [Id(1)] public SplitResult? Split { get; init; }

    /// <summary>
    /// Compares two results by value, with <see cref="ExistingValue"/> compared
    /// by content. The compiler-generated record equality compares the
    /// <see cref="ExistingValue"/> byte array with
    /// <see cref="EqualityComparer{T}.Default"/> (reference equality), so two
    /// structurally identical results built from independently allocated but
    /// byte-identical payloads - and, in particular, a result and its
    /// post-serialization self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The result to compare against.</param>
    public bool Equals(GetOrSetResult? other) =>
        other is not null
        && BytesEqual(ExistingValue, other.ExistingValue)
        && Split == other.Split;

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        if (ExistingValue is { } existing)
        {
            hash.AddBytes(existing);
        }

        if (Split is { } split)
        {
            hash.Add(split);
        }

        return hash.ToHashCode();
    }

    private static bool BytesEqual(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
