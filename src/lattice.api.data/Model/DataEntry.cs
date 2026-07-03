namespace Orleans.Lattice.Api.Data;

/// <summary>
/// A single key / value entry. Used both as an upsert leg of a write batch and
/// as a returned entry of a bounded range read. The value is the full opaque
/// byte payload - unlike the read-only state API's preview-clipped record, the
/// data plane returns and accepts whole values.
/// </summary>
/// <remarks>
/// Deliberately <b>not</b> marked <c>[Immutable]</c>: it carries a mutable
/// value <c>byte[]</c> that, on the write path, is unioned into the grain-bound
/// batch. Leaving the type copy-eligible forces Orleans to deep-copy it across
/// the grain-proxy boundary rather than alias the caller's buffer, matching the
/// safe-copy contract the core <c>LatticeTreeBatch</c> relies on.
/// </remarks>
[GenerateSerializer]
[Alias(DataApiTypeAliases.DataEntry)]
public sealed record DataEntry
{
    /// <summary>The entry key.</summary>
    [Id(0)] public required string Key { get; init; }

    /// <summary>The full value bytes.</summary>
    [Id(1)] public byte[] Value { get; init; } = Array.Empty<byte>();
}
