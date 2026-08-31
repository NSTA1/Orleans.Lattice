namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// The narrow durable key / value surface a <see cref="DurableVectorIndex"/>
/// persists itself through.
/// <para>
/// It is deliberately smaller than any real store: point read, batch read, batch
/// write, batch delete, and two prefix-shaped operations. Scans are expressed as
/// a <i>prefix</i> rather than a key range so the exclusive upper bound is
/// computed once, by the implementation that owns the keyspace, instead of being
/// re-derived at every call site. <see cref="LatticeVectorIndexStore"/> is the
/// implementation that binds this to a Lattice tree.
/// </para>
/// <para>
/// The index needs no multi-key atomicity from an implementation. Durability is
/// established by ordering alone: content records are written first and a single
/// commit record last, so a torn write leaves an uncommitted epoch that the
/// loader ignores and the next flush sweeps.
/// </para>
/// </summary>
public interface IVectorIndexStore
{
    /// <summary>Reads one record, or <see langword="null"/> when the key is absent.</summary>
    /// <param name="key">The full record key.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads several records in one round trip. Absent keys are simply missing
    /// from the result, so a caller can tell an absent record from an empty one.
    /// </summary>
    /// <param name="keys">The full record keys to read.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
        IReadOnlyList<string> keys, CancellationToken cancellationToken = default);

    /// <summary>Writes several records, replacing any that already exist.</summary>
    /// <param name="entries">The records to write.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task WriteAsync(
        IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default);

    /// <summary>Deletes several records. Deleting an absent key is a no-op.</summary>
    /// <param name="keys">The full record keys to delete.</param>
    /// <param name="cancellationToken">Cancels the delete.</param>
    Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates every record whose key starts with <paramref name="keyPrefix"/>,
    /// in ascending ordinal key order.
    /// </summary>
    /// <param name="keyPrefix">The inclusive key prefix.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
        string keyPrefix, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes every record whose key starts with <paramref name="keyPrefix"/>.
    /// Used to retire a superseded index generation, never to touch a store of
    /// record.
    /// </summary>
    /// <param name="keyPrefix">The inclusive key prefix.</param>
    /// <param name="cancellationToken">Cancels the delete.</param>
    Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default);
}
