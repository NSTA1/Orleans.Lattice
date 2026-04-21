using System.Diagnostics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instrumentation for
/// <see cref="BPlusLeafGrain"/>. Wraps <c>IPersistentState.WriteStateAsync</c>
/// in a latency-capturing helper and exposes typed tag builders for the
/// scan, compaction, and tombstone-churn counters defined in
/// <see cref="LatticeMetrics"/>.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Persists the leaf's state and records the elapsed time on the
    /// <see cref="LatticeMetrics.LeafWriteDuration"/> histogram. The tree-id
    /// tag is sourced from persisted state and may be empty when the tree
    /// has not yet been registered with this leaf (pre-<c>SetTreeIdAsync</c>).
    /// </summary>
    private async Task PersistAsync()
    {
        var startTicks = Stopwatch.GetTimestamp();
        try
        {
            await state.WriteStateAsync();
        }
        finally
        {
            var elapsedMs = (Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency;
            LatticeMetrics.LeafWriteDuration.Record(elapsedMs,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty));
        }
    }

    /// <summary>Builds the single-tree tag used by every leaf-level instrument.</summary>
    private KeyValuePair<string, object?> LeafTreeTag() =>
        new(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty);
}