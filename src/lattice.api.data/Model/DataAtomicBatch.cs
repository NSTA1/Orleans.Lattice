namespace Orleans.Lattice.Api.Data;

/// <summary>
/// A single-tree atomic write batch: the key / value <see cref="Upserts"/> and
/// the <see cref="DeleteKeys"/> to commit all-or-nothing on one tree, so no
/// reader ever observes a partial application. The union of the two collections
/// rides one saga; a key may not appear in both, nor more than once within
/// either. An empty batch on both collections is a no-op.
/// </summary>
/// <remarks>
/// Deliberately <b>not</b> marked <c>[Immutable]</c>: it carries mutable
/// reference-typed members (the <see cref="Upserts"/> list and each entry's
/// value buffer) that are unioned into the grain-bound atomic batch, so leaving
/// the type copy-eligible forces Orleans to deep-copy it across the grain-proxy
/// boundary rather than alias the caller's buffers into persisted grain state.
/// </remarks>
[GenerateSerializer]
[Alias(DataApiTypeAliases.DataAtomicBatch)]
public sealed record DataAtomicBatch
{
    /// <summary>The key / value pairs to write atomically. May be empty when the batch is delete-only.</summary>
    [Id(0)] public List<DataEntry> Upserts { get; init; } = [];

    /// <summary>The keys to delete atomically. May be empty when the batch is upsert-only.</summary>
    [Id(1)] public List<string> DeleteKeys { get; init; } = [];
}
