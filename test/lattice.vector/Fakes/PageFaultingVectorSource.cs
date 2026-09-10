using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// A store of record that streams in fixed-size pages and fails a whole page
/// FETCH, which is how a source backed by a remote store actually fails.
/// <para>
/// The distinction is the entire point of the fake. A source reaching its store
/// of record does so a page at a time through one call, and it is that call - not
/// any individual vector inside the page it would have returned - that exceeds
/// the cluster's response timeout under contention. A fake that threw between two
/// yielded items would model a per-item failure and would never reproduce the
/// defect, because by then the items either side of it have already been handed
/// to the index.
/// </para>
/// <para>
/// The fault is raised at a page ordinal counted within a single enumeration, so
/// a resumed enumeration that starts past a banked cursor fails at the same
/// relative position - the shape of a source whose next unread range is the
/// contended one. <c>maxFaults</c> bounds how many enumerations fail, so a test
/// can let a later step succeed and assert on what the earlier one left behind.
/// </para>
/// </summary>
/// <param name="dimensions">The vector width.</param>
/// <param name="pageSize">How many vectors one page fetch returns.</param>
/// <param name="pagesBeforeFault">How many pages are delivered before the failing fetch.</param>
/// <param name="maxFaults">How many enumerations fail before the source becomes healthy.</param>
internal sealed class PageFaultingVectorSource(
    int dimensions, int pageSize, int pagesBeforeFault, int maxFaults) : IVectorSource
{
    private readonly SortedDictionary<string, float[]> _entries = new(StringComparer.Ordinal);
    private int _faultsRemaining = maxFaults;

    public int Dimensions { get; } = dimensions;

    /// <summary>How many entries the last enumeration yielded, so a test can see how far a step read.</summary>
    internal int Yielded { get; private set; }

    /// <summary>How many page fetches have failed, so a test can prove the fault it asked for was raised.</summary>
    internal int FaultsRaised { get; private set; }

    /// <summary>Adds or replaces one vector.</summary>
    internal void Set(string id, float[] vector) => _entries[id] = vector;

    /// <summary>The vector stored under an identifier.</summary>
    internal float[] this[string id] => _entries[id];

    public async IAsyncEnumerable<VectorSourceEntry> EnumerateAsync(
        string? afterIdExclusive, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Yielded = 0;

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
            cancellationToken.ThrowIfCancellationRequested();

            // The page fetch. In the real source this is a grain call, and it is
            // the only await in the loop that can outlive the response timeout.
            await Task.Yield();
            if (offset / pageSize == pagesBeforeFault && _faultsRemaining > 0)
            {
                _faultsRemaining--;
                FaultsRaised++;

                // The exact fault the field measurement recorded, so the fixture
                // fails the way the deployment did rather than the way a fake
                // finds convenient.
                throw new TimeoutException("Response did not arrive on time in 00:00:30.");
            }

            var end = Math.Min(offset + pageSize, remaining.Count);
            for (var i = offset; i < end; i++)
            {
                Yielded++;
                yield return new VectorSourceEntry(remaining[i].Key, remaining[i].Value);
            }
        }
    }

    public Task<int> CountAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(_entries.Count);

    public Task<bool> ContainsAsync(string id, CancellationToken cancellationToken = default) =>
        Task.FromResult(_entries.ContainsKey(id));
}
