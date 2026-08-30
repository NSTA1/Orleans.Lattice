using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// The read surface the fan-out-reduction workload drives: a synthetic key-value
/// store that counts and pays a modelled dispatch hop on every read, and that
/// implements <see cref="IAggregationViewStore"/> so the same instance can back
/// both the reproduced baseline loops and (for the views site) the real shipped
/// applier.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a modelled hop.</b> Every read yields (<see cref="Task.Yield"/>) before
/// answering, so an <c>await</c> completes asynchronously through the thread pool
/// rather than synchronously off a <c>Task.FromResult</c>. A synchronously
/// completing fake would make <c>await</c> free and erase precisely the cost this
/// workload measures: a lattice read is a real grain dispatch, so a loop that
/// awaits N of them in sequence pays N scheduling latencies that a single batched
/// read does not. The scheduler hop is the cheapest faithful stand-in and is
/// therefore a <em>lower bound</em>: on a real silo the per-hop cost is larger and
/// the measured gap widens.
/// </para>
/// <para>
/// <b>The exact figure.</b> Latency under a modelled hop is indicative;
/// <see cref="RoundTrips"/> is exact. Round-trip count is deterministic and host
/// independent, and is the number the batching change actually targets, so the
/// suite reports it alongside the timings rather than inferring it from them.
/// </para>
/// <para>
/// <see cref="GetManyAsync"/> models the two layers a real lattice multi-get has:
/// one caller-visible hop into the tree facade, then - inside it - one concurrent
/// wave of per-shard reads. Only the caller hop counts as a
/// <see cref="RoundTrips"/>; the inner wave is counted apart as
/// <see cref="FanOutReads"/> so the census cannot be read as claiming those reads
/// vanished. They are the same reads, moved behind one facade crossing and issued
/// as one concurrent wave instead of N sequential awaits.
/// </para>
/// </remarks>
internal sealed class FanOutReadSurface : IAggregationViewStore
{
    private readonly Dictionary<string, byte[]> _map;

    /// <summary>Caller-visible read hops since the last <see cref="ResetCounters"/>.</summary>
    public int RoundTrips;

    /// <summary>Single-key reads (<see cref="GetAsync"/>) since the last reset.</summary>
    public int SingleReads;

    /// <summary>Batched reads (<see cref="GetManyAsync"/>) since the last reset.</summary>
    public int BatchedReads;

    /// <summary>
    /// Store-internal per-shard reads issued inside a batched read, since the last
    /// reset. Excluded from <see cref="RoundTrips"/>: they never cross the caller's
    /// facade and are issued as one concurrent wave rather than in sequence.
    /// </summary>
    public int FanOutReads;

    /// <summary>Seeds the surface with the supplied key/value rows.</summary>
    public FanOutReadSurface(IReadOnlyDictionary<string, byte[]> seed)
    {
        _map = new Dictionary<string, byte[]>(seed.Count, StringComparer.Ordinal);
        foreach (var (key, value) in seed)
        {
            _map[key] = value;
        }
    }

    /// <summary>Zeroes every counter ahead of a measured pass.</summary>
    public void ResetCounters()
    {
        RoundTrips = 0;
        SingleReads = 0;
        BatchedReads = 0;
        FanOutReads = 0;
    }

    /// <inheritdoc />
    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref RoundTrips);
        Interlocked.Increment(ref SingleReads);
        await Task.Yield();
        return _map.GetValueOrDefault(key);
    }

    /// <inheritdoc />
    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
    {
        var found = new Dictionary<string, byte[]>(keys.Count, StringComparer.Ordinal);
        if (keys.Count == 0)
        {
            return found;
        }

        Interlocked.Increment(ref RoundTrips);
        Interlocked.Increment(ref BatchedReads);
        await Task.Yield();

        // Store-internal fan-out: one concurrent wave of per-key reads.
        var reads = new Task<byte[]?>[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            reads[i] = ReadShardAsync(keys[i]);
        }

        await Task.WhenAll(reads).ConfigureAwait(false);

        for (var i = 0; i < keys.Count; i++)
        {
            if (await reads[i].ConfigureAwait(false) is { } value)
            {
                found[keys[i]] = value;
            }
        }

        return found;
    }

    private async Task<byte[]?> ReadShardAsync(string key)
    {
        Interlocked.Increment(ref FanOutReads);
        await Task.Yield();
        return _map.GetValueOrDefault(key);
    }

    // The write surface is never driven by the read-fan-out workload; a reach here
    // is a wiring bug, so fail loudly rather than silently no-op.
    Task IAggregationViewStore.SetAsync(string key, byte[] value, CancellationToken cancellationToken) =>
        throw NotDriven();

    Task IAggregationViewStore.DeleteAsync(string key, CancellationToken cancellationToken) =>
        throw NotDriven();

    Task IAggregationViewStore.SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken) =>
        throw NotDriven();

    private static NotSupportedException NotDriven() =>
        new("The read-fan-out workload drives only the store read surface (GetAsync / GetManyAsync).");
}
