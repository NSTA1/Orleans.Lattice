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
    /// Enumerates every record whose key starts with <paramref name="keyPrefix"/>
    /// and sorts strictly after <paramref name="exclusiveStartKey"/>, in ascending
    /// ordinal key order. Passing a null start key is exactly
    /// <see cref="ScanAsync(string, CancellationToken)"/>.
    /// <para>
    /// <b>Why this exists.</b> A corpus-sized walk that faults partway is retried
    /// from the beginning by its caller, so every attempt reissues the reads the
    /// previous attempt already completed. On a tree whose leaves are themselves
    /// slow to activate that regenerates the identical demand indefinitely, which
    /// is the amplification measured on the repository-context planes (#2953).
    /// Resuming past the last record actually consumed lets each attempt bank the
    /// progress it made.
    /// </para>
    /// <para>
    /// <b>Resuming is safe on an ordinal key range and only there.</b> Continuing
    /// from the successor of a key already delivered re-reads nothing and skips
    /// nothing, which is the same justification
    /// <see cref="LatticeVectorIndexStore.ScanAsync(string, CancellationToken)"/>
    /// already relies on for its own in-call resume. It is <b>not</b> a licence to
    /// skip a deliberate re-read: a caller resumes only a walk that was
    /// interrupted, never one it chose to start again.
    /// </para>
    /// <para>
    /// <b>The default implementation is correct but not cheap.</b> It walks from
    /// the prefix and discards what sorts at or below the start key, so an
    /// implementer inherits the right answer without doing anything - but inherits
    /// none of the saving, because the discarded records are still read. Any store
    /// whose underlying range scan accepts a lower bound should override this and
    /// push the bound down; <see cref="LatticeVectorIndexStore"/> does.
    /// </para>
    /// </summary>
    /// <param name="keyPrefix">The inclusive key prefix.</param>
    /// <param name="exclusiveStartKey">
    /// The last key already consumed, which is skipped along with everything
    /// ordering before it, or null to start at the prefix.
    /// </param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
        string keyPrefix, string? exclusiveStartKey, CancellationToken cancellationToken = default)
        => exclusiveStartKey is null
            ? ScanAsync(keyPrefix, cancellationToken)
            : VectorIndexStoreScan.SkipPastAsync(this, keyPrefix, exclusiveStartKey, cancellationToken);

    /// <summary>
    /// Deletes every record whose key starts with <paramref name="keyPrefix"/>.
    /// Used to retire a superseded index generation, never to touch a store of
    /// record.
    /// </summary>
    /// <param name="keyPrefix">The inclusive key prefix.</param>
    /// <param name="cancellationToken">Cancels the delete.</param>
    Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default);
}
