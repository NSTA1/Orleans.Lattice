namespace Orleans.Lattice.Samples.Explorer;

/// <summary>One tree the <see cref="ReplicationWriter"/> writes, and the fixed key set it cycles through.</summary>
/// <param name="Region">The region the tree is written in.</param>
/// <param name="Tree">The tree.</param>
/// <param name="KeyPrefix">The prefix of every key written.</param>
/// <param name="KeyCount">How many distinct keys are written, in turn; the tree never holds more.</param>
internal sealed record ReplicationWriterTarget(string Region, ILattice Tree, string KeyPrefix, int KeyCount)
{
    /// <summary>The key set's size, which must be positive.</summary>
    public int KeyCount { get; } = KeyCount > 0
        ? KeyCount
        : throw new ArgumentOutOfRangeException(nameof(KeyCount), KeyCount, "A writer target needs at least one key.");
}
