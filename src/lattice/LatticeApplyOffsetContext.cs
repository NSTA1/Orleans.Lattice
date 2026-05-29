using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient WAL-offset hint used to stamp the per-prepare
/// offset onto the per-leaf pending-transaction map for projection
/// checkpoint gating. Public callers do not interact with this type -
/// the replay coordinator wraps each <c>ILeafProjection.Apply</c> call
/// in <see cref="BeginScope(long)"/> so the leaf can record the WAL
/// offset of every prepare and clamp checkpoint advances back to
/// <c>min(highest contiguous Apply'd offset, (min unresolved prepare offset) - 1)</c>.
/// Without this clamp, crash recovery could advance the checkpoint past
/// an unresolved saga prepare and silently lose its writes when the
/// terminal mark eventually arrives.
/// </summary>
/// <remarks>
/// <para>
/// The hint flows on the inbound apply path via an Orleans
/// <see cref="RequestContext"/> entry keyed
/// <see cref="LatticeEventConstants.ApplyOffsetRequestContextKey"/>. The
/// foreground commit path leaves the hint unset (<see cref="Current"/>
/// returns <c>null</c>) - there is no WAL offset to stamp because the
/// foreground path is the WAL author, not its replayer, so the leaf's
/// pending-tx offset map simply skips the record.
/// </para>
/// <para>
/// The default outside any scope is <c>null</c>, matching the
/// "wire-compatible default" semantics for foreground writes that
/// existed before the WAL-as-sole-commit-point promotion.
/// </para>
/// </remarks>
internal static class LatticeApplyOffsetContext
{
    /// <summary>
    /// Gets the current ambient WAL offset hint. Returns the
    /// replay-coordinator-stamped offset when an apply scope is active
    /// on the <see cref="RequestContext"/>; otherwise returns
    /// <c>null</c>.
    /// </summary>
    public static long? Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.ApplyOffsetRequestContextKey);
            return raw is long offset ? offset : null;
        }
    }

    /// <summary>
    /// Gets the current ambient WAL partition hint companion to
    /// <see cref="Current"/>. Returns the replay-coordinator-stamped
    /// partition when a partition-aware apply scope is active on the
    /// <see cref="RequestContext"/>; otherwise returns <c>null</c>,
    /// which every reader treats as partition <c>0</c> for wire-compat
    /// with the legacy single-partition shape.
    /// </summary>
    public static int? CurrentPartition
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.ApplyOffsetPartitionRequestContextKey);
            return raw is int partition ? partition : null;
        }
    }

    /// <summary>
    /// Stamps <paramref name="offset"/> as the ambient WAL offset for
    /// the lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent. Equivalent to <see cref="BeginScope(int, long)"/>
    /// with partition <c>0</c> - the legacy single-partition shape.
    /// </summary>
    public static IDisposable BeginScope(long offset)
        => BeginScope(0, offset);

    /// <summary>
    /// Stamps the (<paramref name="partition"/>, <paramref name="offset"/>)
    /// pair as the ambient WAL partition + offset for the lifetime of
    /// the returned scope, restoring the prior values on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent. The partition tag is required for multi-partition
    /// WAL replay so the leaf's per-prepare offset map can clamp the
    /// per-partition projection checkpoint without conflating offsets
    /// from disjoint partition offset spaces.
    /// </summary>
    public static IDisposable BeginScope(int partition, long offset)
    {
        if (partition < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partition), partition, "WAL partition index must be non-negative.");
        }

        var previousOffset = RequestContext.Get(LatticeEventConstants.ApplyOffsetRequestContextKey) as long?;
        var previousPartition = RequestContext.Get(LatticeEventConstants.ApplyOffsetPartitionRequestContextKey) as int?;
        RequestContext.Set(LatticeEventConstants.ApplyOffsetRequestContextKey, offset);
        RequestContext.Set(LatticeEventConstants.ApplyOffsetPartitionRequestContextKey, partition);
        return new Scope(previousOffset, previousPartition);
    }

    private sealed class Scope(long? previousOffset, int? previousPartition) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (previousOffset is null)
            {
                RequestContext.Remove(LatticeEventConstants.ApplyOffsetRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.ApplyOffsetRequestContextKey, previousOffset.Value);
            }

            if (previousPartition is null)
            {
                RequestContext.Remove(LatticeEventConstants.ApplyOffsetPartitionRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.ApplyOffsetPartitionRequestContextKey, previousPartition.Value);
            }
        }
    }
}
