namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// The store of record a <see cref="DurableVectorIndex"/> derives itself from.
/// <para>
/// The index is a <i>projection</i> of this source and is never authoritative
/// over it: a background build streams the source, an incremental update follows
/// it, and any disagreement between the two is resolved by discarding index
/// state and recomputing, never by writing back here. An implementation is
/// therefore only ever read from.
/// </para>
/// </summary>
public interface IVectorSource
{
    /// <summary>
    /// The dimensionality every vector this source yields has. Must match the
    /// index's configured dimensionality.
    /// </summary>
    int Dimensions { get; }

    /// <summary>
    /// Enumerates the source in ascending ordinal identifier order, optionally
    /// resuming strictly after a previously consumed identifier.
    /// <para>
    /// The ordering is what makes a background build resumable: the builder
    /// records the last identifier it durably consumed and re-enters here with
    /// it, so an interrupted build neither repeats nor skips a vector.
    /// </para>
    /// </summary>
    /// <param name="afterIdExclusive">
    /// Resume strictly after this identifier, or <see langword="null"/> to start
    /// at the beginning.
    /// </param>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    IAsyncEnumerable<VectorSourceEntry> EnumerateAsync(
        string? afterIdExclusive, CancellationToken cancellationToken = default);

    /// <summary>
    /// The number of vectors the source currently holds, used only to report
    /// build progress. An implementation may return a bound rather than an exact
    /// figure; nothing depends on it for correctness.
    /// </summary>
    /// <param name="cancellationToken">Cancels the count.</param>
    Task<int> CountAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Whether the source still holds a vector under an identifier. This is the
    /// authority a coherence sweep tests the index against.
    /// </summary>
    /// <param name="id">The source identifier.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    Task<bool> ContainsAsync(string id, CancellationToken cancellationToken = default);
}
