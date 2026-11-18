namespace Orleans.Lattice.Views;

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

    // key -> value last read from the underlying tree, with a null value marking
    // a key the tree does not hold. Every write in a buffered window lands in the
    // overlay and none reaches the tree, and the maintainer is the view tree's
    // only writer, so a row read once cannot change before Capture(). Caching it
    // means a batch re-reads nothing it has already read: the materialise gather
    // re-probed every untouched accumulator slot, and every contribution re-read
    // the absent intent and membership rows, on each contribution to a group.
    // Consulted only after the overlay, so a buffered write always wins; kept
    // apart from it so Capture() still yields exactly the writes. The applier
    // overlaps its point reads, so cache access is locked: inside a grain every
    // continuation runs on the activation's turn anyway, but a caller off the
    // grain scheduler must not be able to corrupt the dictionary.
    private readonly Dictionary<string, byte[]?> _reads = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        if (_overlay.TryGetValue(key, out var buffered))
        {
            return buffered;
        }

        lock (_reads)
        {
            if (_reads.TryGetValue(key, out var read))
            {
                return read;
            }
        }

        var fetched = await inner.GetAsync(key, cancellationToken);
        lock (_reads)
        {
            _reads[key] = fetched;
        }

        return fetched;
    }

    /// <inheritdoc />
    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        List<string>? misses = null;
        foreach (var key in keys)
        {
            if (_overlay.TryGetValue(key, out var buffered))
            {
                // A buffered null marks a delete, so the key reads as absent.
                if (buffered is not null)
                {
                    result[key] = buffered;
                }
            }
            else if (TryGetRead(key, out var read))
            {
                if (read is not null)
                {
                    result[key] = read;
                }
            }
            else
            {
                (misses ??= []).Add(key);
            }
        }

        if (misses is not null)
        {
            var fetched = await inner.GetManyAsync(misses, cancellationToken);
            lock (_reads)
            {
                foreach (var key in misses)
                {
                    if (fetched.TryGetValue(key, out var value))
                    {
                        result[key] = value;
                        _reads[key] = value;
                    }
                    else
                    {
                        _reads[key] = null;
                    }
                }
            }
        }

        return result;
    }

    private bool TryGetRead(string key, out byte[]? value)
    {
        lock (_reads)
        {
            return _reads.TryGetValue(key, out value);
        }
    }

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
