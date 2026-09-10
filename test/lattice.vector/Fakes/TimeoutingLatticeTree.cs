namespace Orleans.Lattice.Vector.Tests.Fakes;

using NSubstitute;

/// <summary>
/// An <see cref="ILattice"/> that walks an ordinal key range the way a real tree
/// does, but abandons a walk part way through with the bare
/// <see cref="TimeoutException"/> the Orleans runtime raises when a response does
/// not arrive in time.
/// <para>
/// This models the fault measured in the field on the durable index's reload
/// path: a page fill queued behind other work on a non-reentrant shard root, so
/// the runtime's response timeout fires before any of the scan's own bounds - all
/// of which measure from the first statement inside the grain call - can raise
/// the typed, resumable <c>ScanPageStalledException</c> instead.
/// </para>
/// <para>
/// It records the lower bound of every walk it is asked for, which is what makes
/// "resumed rather than restarted" directly assertable: a restart re-enters at
/// the original prefix, whereas a resume re-enters at the successor of the last
/// key actually delivered.
/// </para>
/// </summary>
internal sealed class TimeoutingLatticeTree
{
    private readonly SortedDictionary<string, byte[]> _records = new(StringComparer.Ordinal);
    private readonly int _entriesBeforeTimeout;
    private readonly int _deliverBeforeWedging;
    private int _remainingTimeouts;

    private TimeoutingLatticeTree(int entriesBeforeTimeout, int timeouts, int deliverBeforeWedging)
    {
        _entriesBeforeTimeout = entriesBeforeTimeout;
        _remainingTimeouts = timeouts;
        _deliverBeforeWedging = deliverBeforeWedging;
        Tree = Build();
    }

    /// <summary>The tree to hand to a <see cref="Persistence.LatticeVectorIndexStore"/>.</summary>
    internal ILattice Tree { get; }

    /// <summary>
    /// The lower bound of every walk requested, in order. The first is the
    /// original prefix; each later one is where that walk resumed from.
    /// </summary>
    internal List<string?> StartBounds { get; } = [];

    /// <summary>
    /// How many records the tree has handed out in total, across every walk. A
    /// resume that re-read what it had already delivered would push this above
    /// the record count; an exact resume leaves it equal to it.
    /// </summary>
    internal int RecordsDelivered { get; private set; }

    /// <summary>
    /// Creates a tree holding <paramref name="keys"/>, which abandons each of its
    /// first <paramref name="timeouts"/> walks after
    /// <paramref name="entriesBeforeTimeout"/> records.
    /// <para>
    /// <paramref name="deliverBeforeWedging"/> lets a tree deliver normally for a
    /// while and only then start abandoning, which is how a walk that banked real
    /// progress before the tree became unavailable is modelled.
    /// </para>
    /// </summary>
    internal static TimeoutingLatticeTree Create(
        IEnumerable<string> keys,
        int entriesBeforeTimeout,
        int timeouts,
        int deliverBeforeWedging = 0)
    {
        var tree = new TimeoutingLatticeTree(entriesBeforeTimeout, timeouts, deliverBeforeWedging);
        var ordinal = 0;
        foreach (var key in keys)
        {
            tree._records[key] = [(byte)(ordinal++ & 0xFF)];
        }

        return tree;
    }

    private ILattice Build()
    {
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var start = call.ArgAt<string?>(0);
                var end = call.ArgAt<string?>(1);
                StartBounds.Add(start);

                // Materialise the range before yielding, exactly as the ordinal
                // fake does: a real page fill has already selected its range by
                // the time it starts handing records back.
                var page = _records
                    .Where(entry =>
                        (start is null || string.CompareOrdinal(entry.Key, start) >= 0)
                        && (end is null || string.CompareOrdinal(entry.Key, end) < 0))
                    .ToArray();

                var abandon = _remainingTimeouts > 0;
                if (abandon)
                {
                    _remainingTimeouts--;
                }

                return Walk(page, abandon);
            });

        return tree;
    }

    private async IAsyncEnumerable<KeyValuePair<string, byte[]>> Walk(
        KeyValuePair<string, byte[]>[] page, bool abandon)
    {
        var deliveredThisWalk = 0;
        for (var i = 0; i < page.Length; i++)
        {
            if (abandon
                && RecordsDelivered >= _deliverBeforeWedging
                && deliveredThisWalk >= _entriesBeforeTimeout)
            {
                // The message is the runtime's own, so a reader of a failing test
                // sees the string the field logs actually carried.
                throw new TimeoutException("Response did not arrive on time in 00:00:30.");
            }

            RecordsDelivered++;
            deliveredThisWalk++;
            yield return page[i];
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }
}
