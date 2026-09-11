using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// A source that answers promptly but never synchronously: every item is handed
/// over after a yield, so each <c>MoveNextAsync</c> completes asynchronously.
/// <para>
/// This is the shape neither existing fake covers, and the gap matters because
/// the two are bounded by different code. <c>ListVectorSource</c> completes every
/// read synchronously, so a slice reading it never waits and never meets the
/// deadline at all. <c>StallingVectorSource</c> never completes the read it
/// stalls on, so a slice reading it always meets the deadline. A real store of
/// record is neither: it answers, and it answers asynchronously, so its reads go
/// down the waiting path and ARE governed by the deadline while still being
/// perfectly healthy.
/// </para>
/// <para>
/// That is the case in which the window the deadline is armed with is the only
/// thing standing between a working build and a stalled one, which is what this
/// fake exists to expose. See
/// <c>DurableVectorIndexSliceBudgetTests.A_slice_that_has_banked_nothing_is_armed_with_the_full_budget</c>.
/// </para>
/// </summary>
/// <param name="dimensions">The vector width.</param>
internal sealed class DeferredVectorSource(int dimensions) : IVectorSource
{
    private readonly SortedDictionary<string, float[]> _entries = new(StringComparer.Ordinal);

    public int Dimensions { get; } = dimensions;

    /// <summary>Adds or replaces one vector.</summary>
    internal void Set(string id, float[] vector) => _entries[id] = vector;

    public async IAsyncEnumerable<VectorSourceEntry> EnumerateAsync(
        string? afterIdExclusive, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var entry in _entries)
        {
            if (afterIdExclusive is not null && string.CompareOrdinal(entry.Key, afterIdExclusive) <= 0)
            {
                continue;
            }

            // Forces the read onto the waiting path without making it slow: the
            // continuation is already queued when the caller starts to wait.
            await Task.Yield();
            cancellationToken.ThrowIfCancellationRequested();
            yield return new VectorSourceEntry(entry.Key, entry.Value);
        }
    }

    public Task<int> CountAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(_entries.Count);

    public Task<bool> ContainsAsync(string id, CancellationToken cancellationToken = default) =>
        Task.FromResult(_entries.ContainsKey(id));
}
