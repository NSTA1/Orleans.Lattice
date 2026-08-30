using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// The baseline (pre-change) and batched (shipped) read shapes of the three
/// fan-out sites this workload compares, expressed over the same
/// <see cref="FanOutReadSurface"/> so the only difference between each pair is the
/// call shape. Each reproduces one production loop verbatim; the shipped arm is
/// the single batched <see cref="FanOutReadSurface.GetManyAsync"/> the change
/// replaced it with.
/// </summary>
/// <remarks>
/// The three production sites and the loops reproduced here:
/// <list type="bullet">
/// <item><description><b>Tag-index AND query</b> -
/// <c>LatticeTagIndexContext.QueryAsync</c> (all-tags branch): for each candidate
/// key from the first tag's posting list, probe the other (T-1) tags' membership
/// rows. Baseline awaited one <c>ExistsAsync</c>/<c>GetAsync</c> per sibling tag;
/// shipped issues one batched multi-get per candidate.</description></item>
/// <item><description><b>Atomic-action pre-image</b> -
/// <c>AtomicActionGrain.RunTreeWriteForwardAsync</c>: capture each written key's
/// pre-image. Baseline awaited one <c>GetAsync</c> per key; shipped issues one
/// batched multi-get for the whole step.</description></item>
/// <item><description><b>Aggregation-view inverse materialisation</b> -
/// <c>AggregationApplier.MaterialiseInverseAsync</c> /
/// <c>MaterialiseFoldAsync</c>: gather a group's inverse shards. Baseline awaited
/// one <c>GetAsync</c> per shard slot; shipped issues one batched multi-get for
/// all slots.</description></item>
/// </list>
/// </remarks>
internal static class FanOutShapes
{
    // ------------------------------------------------------------------
    // Tag-index AND query: (T-1) sibling-tag membership probes per candidate key.
    // ------------------------------------------------------------------

    /// <summary>Baseline: one sequential membership read per sibling tag, per candidate.</summary>
    public static async Task<int> TagIndexAndBaselineAsync(
        FanOutReadSurface store,
        IReadOnlyList<IReadOnlyList<string>> candidateProbeKeys)
    {
        var matched = 0;
        foreach (var probe in candidateProbeKeys)
        {
            var inAll = true;
            for (var i = 0; i < probe.Count && inAll; i++)
            {
                inAll = await store.GetAsync(probe[i]).ConfigureAwait(false) is not null;
            }

            if (inAll)
            {
                matched++;
            }
        }

        return matched;
    }

    /// <summary>Shipped: one batched membership multi-get per candidate.</summary>
    public static async Task<int> TagIndexAndBatchedAsync(
        FanOutReadSurface store,
        IReadOnlyList<IReadOnlyList<string>> candidateProbeKeys)
    {
        var matched = 0;
        var probe = new List<string>();
        foreach (var siblings in candidateProbeKeys)
        {
            probe.Clear();
            for (var i = 0; i < siblings.Count; i++)
            {
                probe.Add(siblings[i]);
            }

            var rows = await store.GetManyAsync(probe).ConfigureAwait(false);
            var inAll = true;
            for (var i = 0; i < probe.Count; i++)
            {
                if (!rows.ContainsKey(probe[i]))
                {
                    inAll = false;
                    break;
                }
            }

            if (inAll)
            {
                matched++;
            }
        }

        return matched;
    }

    // ------------------------------------------------------------------
    // Atomic-action pre-image: one read per written key.
    // ------------------------------------------------------------------

    /// <summary>Baseline: one sequential read per key.</summary>
    public static async Task<int> AtomicPreImageBaselineAsync(FanOutReadSurface store, List<string> keys)
    {
        var existed = 0;
        foreach (var key in keys)
        {
            if (await store.GetAsync(key).ConfigureAwait(false) is not null)
            {
                existed++;
            }
        }

        return existed;
    }

    /// <summary>Shipped: one batched read for the whole step.</summary>
    public static async Task<int> AtomicPreImageBatchedAsync(FanOutReadSurface store, List<string> keys)
    {
        var current = await store.GetManyAsync(keys).ConfigureAwait(false);
        var existed = 0;
        foreach (var key in keys)
        {
            if (current.ContainsKey(key))
            {
                existed++;
            }
        }

        return existed;
    }

    // ------------------------------------------------------------------
    // Aggregation-view inverse materialisation: one read per shard slot.
    // ------------------------------------------------------------------

    /// <summary>Baseline: one sequential read per shard slot.</summary>
    public static async Task<int> ViewInverseBaselineAsync(FanOutReadSurface store, List<string> slotKeys)
    {
        var rows = 0;
        foreach (var key in slotKeys)
        {
            if (await store.GetAsync(key).ConfigureAwait(false) is not null)
            {
                rows++;
            }
        }

        return rows;
    }

    /// <summary>Shipped: one batched read across all shard slots.</summary>
    public static async Task<int> ViewInverseBatchedAsync(FanOutReadSurface store, List<string> slotKeys)
    {
        var shards = await store.GetManyAsync(slotKeys).ConfigureAwait(false);
        return shards.Count;
    }
}
