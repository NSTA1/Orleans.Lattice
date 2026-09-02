using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// Binds <see cref="IVectorIndexStore"/> to a Lattice tree, so a persisted index
/// is ordinary Lattice data: sharded, replicated, backed up, and reclaimed by the
/// same machinery as everything else in the store.
/// <para>
/// This is the only type in the package that touches Orleans or a Lattice tree.
/// The algorithmic core in <c>Orleans.Lattice.Vector</c> and the whole durable
/// engine above this seam are free of both, so they can be exercised - and
/// reused - without a silo.
/// </para>
/// <para>
/// Give the index its own tree, or at least its own key prefix. It is a derived
/// projection and its recovery path is to delete a whole key range and rebuild;
/// pointing it at a tree holding anything else would put a store of record in
/// range of that.
/// </para>
/// </summary>
/// <param name="tree">The Lattice tree the index is persisted on. Must not be <see langword="null"/>.</param>
public sealed class LatticeVectorIndexStore(ILattice tree) : IVectorIndexStore
{
    private readonly ILattice _tree = tree ?? throw new ArgumentNullException(nameof(tree));

    /// <inheritdoc />
    public Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _tree.GetAsync(key, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, byte[]>> ReadManyAsync(
        IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (keys.Count == 0)
        {
            return EmptyRecords;
        }

        return await _tree.GetManyAsync([.. keys], cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task WriteAsync(
        IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        return entries.Count == 0
            ? Task.CompletedTask
            : _tree.SetManyAsync([.. entries], cancellationToken);
    }

    /// <inheritdoc />
    public async Task DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        for (var i = 0; i < keys.Count; i++)
        {
            await _tree.DeleteAsync(keys[i], cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Walked through <see cref="LatticeExtensions.ScanEntriesAsync"/> rather than
    /// the raw <see cref="ILattice.EntriesAsync"/> stream. This is the walk that
    /// RELOADS a persisted index at open, so its range grows with the index and
    /// therefore with the corpus - precisely the shape the raw stream warns about,
    /// because it surfaces <c>EnumerationAbortedException</c> when the remote
    /// enumerator is reclaimed mid-scan (silo failover, cold start, idle expiry,
    /// scale-down). An abort here does not degrade the reload, it FAILS it, and the
    /// index then rebuilds from scratch or refuses to serve - which is the opposite
    /// of the "reload instead of recompute" property the persisted index exists to
    /// provide. The resilient wrapper resumes deterministically with no duplicates
    /// and no gaps. The same defect was measured firing for real on the repocontext
    /// count path against a restored copy of a live deployment (#1844).
    /// </remarks>
    public async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanAsync(
        string keyPrefix, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keyPrefix);

        // A "starts with keyPrefix" query is exactly the half-open ordinal range
        // [keyPrefix, PrefixUpperBound(keyPrefix)). The shared helper owns that
        // bound - incrementing the last code unit by hand wraps a trailing U+FFFF
        // to U+0000 and silently inverts the range - and returns null when the
        // range has no finite upper bound, which the scan primitives take to mean
        // "run to the end of the keyspace".
        var upperBound = LatticeKeyRange.PrefixUpperBound(keyPrefix);
        var entries = _tree.ScanEntriesAsync(keyPrefix, upperBound, cancellationToken: cancellationToken);
        await foreach (var entry in entries.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return entry;
        }
    }

    /// <inheritdoc />
    public async Task DeletePrefixAsync(string keyPrefix, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keyPrefix);

        var upperBound = LatticeKeyRange.PrefixUpperBound(keyPrefix);
        if (upperBound is not null)
        {
            await _tree.DeleteRangeAsync(keyPrefix, upperBound, cancellationToken).ConfigureAwait(false);
            return;
        }

        // No finite upper bound exists, so the range runs to the end of the
        // keyspace and the range-delete primitive cannot express it. Enumerating
        // first is correct here because the keys are collected before any delete
        // is issued, so the walk is not invalidated underneath itself. The walk
        // uses the abort-resilient wrapper for the same reason ScanAsync does: an
        // aborted enumerator would yield a SHORT key list, and a short list here
        // means a partial delete that silently leaves a superseded generation
        // behind rather than failing.
        var keys = new List<string>();
        var enumerator = _tree.ScanKeysAsync(keyPrefix, null, cancellationToken: cancellationToken);
        await foreach (var key in enumerator.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            keys.Add(key);
        }

        await DeleteAsync(keys, cancellationToken).ConfigureAwait(false);
    }

    private static readonly Dictionary<string, byte[]> EmptyRecords = [];
}
