namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Outcome of <see cref="IShardRootGrain.TryGetOptimisticAsync"/>: either a validated
/// point-read value (which may be <c>null</c> for an absent or tombstoned key) or a
/// request that the caller repeat the read on the serial
/// <see cref="IShardRootGrain.GetAsync"/> path.
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
    /// epoch and <see cref="Value"/> is authoritative; <c>false</c> when the caller
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
