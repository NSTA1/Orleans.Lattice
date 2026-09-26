namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Outcome of <see cref="IShardRootGrain.TryGetOptimisticAsync"/>: either a validated
/// point-read value or a request that the caller repeat the read on the serial
/// <see cref="IShardRootGrain.GetAsync"/> path. A validated <c>null</c> proves
/// absence in a leaf-owned range, and callers treat it as authoritative.
/// <para>
/// The <c>default</c> instance means "repeat on the serial path", so an
/// uninitialised or default-returning result can never be mistaken for a
/// validated absent key.
/// </para>
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.OptimisticReadResult)]
internal readonly record struct OptimisticReadResult
{
    /// <summary>
    /// The value read, or <c>null</c> when the key is absent or tombstoned.
    /// Meaningful only when <see cref="IsValidated"/> is <c>true</c>.
    /// </summary>
    [Id(0)]
    public byte[]? Value { get; init; }

    /// <summary>
    /// <c>true</c> when the read was validated against the shard root's routing
    /// epoch and either no overlapping point write or the leaf's ownership stamp, making <see cref="Value"/>
    /// authoritative; <c>false</c> when the caller
    /// must repeat the read through the serial <see cref="IShardRootGrain.GetAsync"/>.
    /// </summary>
    [Id(1)]
    public bool IsValidated { get; init; }

    /// <summary>The "repeat on the serial path" result (equal to <c>default</c>).</summary>
    public static OptimisticReadResult SerialRetry => default;

    /// <summary>Creates a validated result carrying <paramref name="value"/>.</summary>
    /// <param name="value">The value read, or <c>null</c> when absent.</param>
    public static OptimisticReadResult FromValue(byte[]? value) => new() { Value = value, IsValidated = true };
}
