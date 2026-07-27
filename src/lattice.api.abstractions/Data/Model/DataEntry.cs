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

    /// <summary>
    /// The per-key convergence discriminator, or <see langword="null"/>. Populated
    /// only on the point-read path (<see cref="DataReadResult"/>); on the bulk range
    /// read it is left <see langword="null"/> because the range cursor does not carry
    /// per-key modes and resolving one per entry would add a round-trip to every row
    /// of the cheap bulk path. For a self-describing per-key read use the point read
    /// (<c>get</c>) or the read-only state API's <c>scan_entries</c>, which decode
    /// members directly. Ignored on the write (upsert) path.
    /// </summary>
    [Id(2)] public LatticeMergeMode? MergeMode { get; init; }

    /// <summary>
    /// On a returned range entry, always <see langword="true"/>: the data plane
    /// returns the raw stored bytes verbatim and never decodes a typed CRDT into a
    /// logical projection, so a consumer must not treat <see cref="Value"/> as a
    /// decoded value. Because the bulk range read does not resolve
    /// <see cref="MergeMode"/>, use the point read or the state API when you need to
    /// know whether the bytes are a plain value or internal CRDT state. Ignored on
    /// the write (upsert) path (defaults to <see langword="false"/>).
    /// </summary>
    [Id(3)] public bool Raw { get; init; }
}
