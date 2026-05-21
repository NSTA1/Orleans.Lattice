using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient context that carries the compaction trigger label
/// (<c>reminder</c> / <c>ratio</c> / <c>size</c> / <c>operator</c>) into
/// <see cref="BPlusTree.Grains.BPlusLeafGrain"/>'s
/// <c>CompactTombstonesAsync</c> so per-leaf instruments
/// (<see cref="LatticeMetrics.LeafCompactionDuration"/>,
/// <see cref="LatticeMetrics.LeafTombstonesReaped"/>,
/// <see cref="LatticeMetrics.LeafTombstonesExpired"/>,
/// <see cref="LatticeMetrics.CompactionLeavesVisited"/>) can be tagged
/// with the originating trigger when at least one policy knob is
/// non-default. Public callers do not interact with this type.
/// </summary>
/// <remarks>
/// <para>
/// Flows on the inbound call path through Orleans
/// <see cref="RequestContext"/> keyed
/// <see cref="LatticeEventConstants.CompactionTriggerRequestContextKey"/>.
/// <see cref="BPlusTree.Grains.TombstoneCompactionGrain"/> wraps each
/// shard-walk with a <see cref="BeginScope"/> so every leaf call inside
/// the pass observes the trigger label. Outside any scope
/// <see cref="Current"/> returns <see langword="null"/> and tag emission
/// falls back to the v3.4.0 no-tag shape so existing dashboards continue
/// to match.
/// </para>
/// </remarks>
internal static class LatticeCompactionTriggerContext
{
    /// <summary>
    /// Gets the current ambient trigger label, or <see langword="null"/>
    /// when no trigger scope is active.
    /// </summary>
    public static string? Current
        => RequestContext.Get(LatticeEventConstants.CompactionTriggerRequestContextKey) as string;

    /// <summary>
    /// Marks the ambient context with the given trigger label for the
    /// lifetime of the returned scope, restoring the prior value on
    /// disposal. Safe to nest; disposal is idempotent.
    /// </summary>
    public static IDisposable BeginScope(string triggerKind)
    {
        ArgumentNullException.ThrowIfNull(triggerKind);
        var previous = RequestContext.Get(LatticeEventConstants.CompactionTriggerRequestContextKey) as string;
        RequestContext.Set(LatticeEventConstants.CompactionTriggerRequestContextKey, triggerKind);
        return new Scope(previous);
    }

    private sealed class Scope(string? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            if (previous is null)
            {
                RequestContext.Remove(LatticeEventConstants.CompactionTriggerRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.CompactionTriggerRequestContextKey, previous);
            }
        }
    }
}
