namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// The fallback resume used by
/// <see cref="IVectorIndexStore.ScanAsync(string, string?, CancellationToken)"/>
/// when a store has not pushed the lower bound into its own range scan.
/// </summary>
internal static class VectorIndexStoreScan
{
    /// <summary>
    /// Walks the prefix and yields only what sorts strictly after
    /// <paramref name="exclusiveStartKey"/>.
    /// <para>
    /// This delivers the correct records and none of the saving, which is the
    /// deliberate trade for a default that cannot be wrong. The point of resuming
    /// is to stop reissuing reads the previous attempt already completed, and a
    /// filter applied after the read has already paid for it - so a store that
    /// leaves this in place still amplifies demand exactly as before. It is safe
    /// to inherit and wrong to settle for; see the override on
    /// <see cref="LatticeVectorIndexStore"/>.
    /// </para>
    /// <para>
    /// The comparison is ordinal because the key range it resumes over is ordinal.
    /// A culture-sensitive comparison here would order keys differently from the
    /// scan that produced them, and the resume would then skip or repeat records
    /// according to the host's locale.
    /// </para>
    /// </summary>
    internal static async IAsyncEnumerable<KeyValuePair<string, byte[]>> SkipPastAsync(
        IVectorIndexStore store,
        string keyPrefix,
        string exclusiveStartKey,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(keyPrefix);
        ArgumentNullException.ThrowIfNull(exclusiveStartKey);

        var scan = store.ScanAsync(keyPrefix, cancellationToken);
        await foreach (var entry in scan.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (string.CompareOrdinal(entry.Key, exclusiveStartKey) <= 0)
            {
                continue;
            }

            yield return entry;
        }
    }
}
