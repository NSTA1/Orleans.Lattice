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
}
