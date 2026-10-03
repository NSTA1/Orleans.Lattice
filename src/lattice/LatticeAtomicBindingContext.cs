using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient that binds an atomic-write saga's prepared writes to the
/// physical tree the saga is bound to (issue #4358).
/// </summary>
/// <remarks>
/// <para>
/// The saga dispatches its prepared batch through the stateless routing tier
/// (<c>LatticeGrain.SetManyAsync</c>), whose activations each cache a
/// logical-to-physical routing pair. Across an alias swap - a resize, a resize
/// undo, an explicit alias, a restore or its revert - an activation that has not
/// yet observed the swap still addresses the previous copy, so a saga that had
/// re-bound to the new copy could have its prepares placed on the old one. The
/// commit decision and terminals then went to the bound copy, which held no
/// prepare for those keys, and the batch reached it only through the per-shard
/// terminal backstop, shard by shard: a transient tear, or a lost batch.
/// </para>
/// <para>
/// The saga therefore stamps its bound physical tree around the dispatch with
/// <see cref="With"/>. The routing tier <see cref="Take"/>s it on receipt, so it
/// never travels to the shards, re-reads its routing from the registry when its
/// cached pair addresses another copy, and refuses to place the batch anywhere
/// but the bound copy: it raises <see cref="StaleTreeRoutingException"/> whose
/// <see cref="StaleTreeRoutingException.StalePhysicalTreeId"/> is the bound copy,
/// and the saga re-binds and re-dispatches. The value only ever narrows where a
/// write may land, so it confers no capability.
/// </para>
/// </remarks>
internal static class LatticeAtomicBindingContext
{
    /// <summary>
    /// Gets the bound physical tree id on the current <see cref="RequestContext"/>,
    /// or <see langword="null"/> when no binding is in scope.
    /// </summary>
    public static string? Current =>
        RequestContext.Get(LatticeEventConstants.AtomicBoundPhysicalTreeRequestContextKey) as string;

    /// <summary>
    /// Stamps <paramref name="physicalTreeId"/> as the bound physical tree for the
    /// lifetime of the returned scope, restoring the prior value on disposal.
    /// A <see langword="null"/> or empty id clears the binding.
    /// </summary>
    public static IDisposable With(string? physicalTreeId)
    {
        var previous = Current;
        Set(physicalTreeId);
        return new Scope(previous);
    }

    /// <summary>
    /// Returns the bound physical tree id and removes it from the current
    /// <see cref="RequestContext"/>, so calls made from here on do not carry it.
    /// </summary>
    public static string? Take()
    {
        var current = Current;
        if (current is not null)
        {
            RequestContext.Remove(LatticeEventConstants.AtomicBoundPhysicalTreeRequestContextKey);
        }

        return current;
    }

    private static void Set(string? physicalTreeId)
    {
        if (string.IsNullOrEmpty(physicalTreeId))
        {
            RequestContext.Remove(LatticeEventConstants.AtomicBoundPhysicalTreeRequestContextKey);
        }
        else
        {
            RequestContext.Set(LatticeEventConstants.AtomicBoundPhysicalTreeRequestContextKey, physicalTreeId);
        }
    }

    private sealed class Scope(string? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Set(previous);
        }
    }
}
