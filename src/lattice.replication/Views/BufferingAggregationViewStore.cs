namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// An <see cref="IAggregationViewStore"/> decorator that buffers every write in
/// an in-memory overlay instead of applying it to the underlying view tree, while
/// serving reads from that overlay layered over the real tree. Running an
/// aggregation batch's contributions through an <see cref="AggregationApplier"/>
/// backed by this store therefore <i>materialises</i> the net set of view-tree
/// row writes the applier's read-before-write flips would produce - the
/// aggregation view's <b>slice</b> - without touching the live tree. The captured
/// slice is then contributed to the cross-tree joint flip (its upserts) with its
/// retractions applied afterwards, exactly as the filter path coalesces and
/// flips its projected writes.
/// <para>
/// Because reads consult the overlay first, contributions later in the same batch
/// observe the accumulator / membership rows advanced by earlier contributions,
/// so the captured slice is the correct net final state for every touched key -
/// identical bytes to what the live applier would have written.
/// </para>
/// </summary>
internal sealed class BufferingAggregationViewStore(IAggregationViewStore inner) : IAggregationViewStore
{
    // key -> value, with a null value marking a delete. Tracks the net write per
    // key across the whole batch (a later write to the same key overwrites the
    // earlier one), so the overlay is exactly the captured slice.
    private readonly Dictionary<string, byte[]?> _overlay = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        _overlay.TryGetValue(key, out var buffered)
            ? buffered
            : await inner.GetAsync(key, cancellationToken);

    /// <inheritdoc />
    public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
    {
        _overlay[key] = value;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        _overlay[key] = null;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default)
    {
        foreach (var entry in entries)
        {
            _overlay[entry.Key] = entry.Value;
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Materialises the buffered overlay into the cross-tree slice: the
    /// non-deleted rows become the upserts the joint flip sets, and the deleted
    /// rows become the retractions the maintainer applies after the joint flip.
    /// </summary>
    public (List<KeyValuePair<string, byte[]>> Upserts, List<string> Deletes) Capture()
    {
        var upserts = new List<KeyValuePair<string, byte[]>>(_overlay.Count);
        var deletes = new List<string>();
        foreach (var (key, value) in _overlay)
        {
            if (value is null)
            {
                deletes.Add(key);
            }
            else
            {
                upserts.Add(new KeyValuePair<string, byte[]>(key, value));
            }
        }

        return (upserts, deletes);
    }
}
