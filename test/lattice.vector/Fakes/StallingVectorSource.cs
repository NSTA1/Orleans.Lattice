using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// A store of record whose page fetch STALLS rather than faults: it never
/// returns, and never yields the items the page would have carried, until the
/// caller's token is cancelled.
/// <para>
/// This is the fake the existing ones cannot stand in for, and the difference is
/// the whole defect. <c>PageFaultingVectorSource</c> throws, which is an answer -
/// the slice unwinds and every item it had already taken is still in hand.
/// <c>ListVectorSource</c> answers immediately. Neither reproduces a source that
/// simply does not respond, which is what a grain call queued behind a contended
/// non-reentrant shard root actually does, and which is the case that defeats
/// both a budget sampled after an item and a checkpoint conditioned on having
/// consumed one.
/// </para>
/// <para>
/// With <paramref name="pagesBeforeStall"/> of zero the source yields NOTHING at
/// all, which is the measured shape: a build that cannot read its first page
/// consumes nothing, banks nothing, advances no cursor, and re-reads the same
/// range on every step forever.
/// </para>
/// </summary>
/// <param name="dimensions">The vector width.</param>
/// <param name="pageSize">How many vectors one page fetch returns.</param>
/// <param name="pagesBeforeStall">How many pages are delivered before the fetch that never returns.</param>
internal sealed class StallingVectorSource(int dimensions, int pageSize, int pagesBeforeStall) : IVectorSource
{
    private readonly SortedDictionary<string, float[]> _entries = new(StringComparer.Ordinal);
    private readonly TaskCompletionSource _stalled =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int Dimensions { get; } = dimensions;

    /// <summary>How many entries the last enumeration yielded, so a test can see how far a step read.</summary>
    internal int Yielded { get; private set; }

    /// <summary>How many enumerations reached the stalling fetch.</summary>
    internal int Stalls { get; private set; }

    /// <summary>
    /// Completes once a stalled fetch has been observed, so a test can wait for
    /// the stall to be reached instead of sleeping and hoping.
    /// </summary>
    internal Task Stalled => _stalled.Task;

    /// <summary>Whether every enumerator handed out has been disposed.</summary>
    internal bool AllEnumeratorsDisposed => Volatile.Read(ref _live) == 0;

    private int _live;

    /// <summary>Adds or replaces one vector.</summary>
    internal void Set(string id, float[] vector) => _entries[id] = vector;

    /// <summary>The vector stored under an identifier.</summary>
    internal float[] this[string id] => _entries[id];

    public async IAsyncEnumerable<VectorSourceEntry> EnumerateAsync(
        string? afterIdExclusive, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Yielded = 0;
        Interlocked.Increment(ref _live);
        try
        {
            var remaining = new List<KeyValuePair<string, float[]>>();
            foreach (var entry in _entries)
            {
                if (afterIdExclusive is null || string.CompareOrdinal(entry.Key, afterIdExclusive) > 0)
                {
                    remaining.Add(entry);
                }
            }

            for (var offset = 0; offset < remaining.Count; offset += pageSize)
            {
                if (offset / pageSize == pagesBeforeStall)
                {
                    Stalls++;
                    _stalled.TrySetResult();

                    // The page fetch that never returns. Only cancellation ends
                    // it, which is exactly the leverage the slice deadline has to
                    // have for its bound to mean anything.
                    await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
                }

                var end = Math.Min(offset + pageSize, remaining.Count);
                for (var i = offset; i < end; i++)
                {
                    Yielded++;
                    yield return new VectorSourceEntry(remaining[i].Key, remaining[i].Value);
                }
            }

            if (remaining.Count == 0 && pagesBeforeStall == 0)
            {
                Stalls++;
                _stalled.TrySetResult();
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            Interlocked.Decrement(ref _live);
        }
    }

    public Task<int> CountAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(_entries.Count);

    public Task<bool> ContainsAsync(string id, CancellationToken cancellationToken = default) =>
        Task.FromResult(_entries.ContainsKey(id));
}
