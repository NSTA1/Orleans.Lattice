using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient atomic-batch context used to stamp
/// <see cref="LatticeMutation.AtomicBatchSize"/> /
/// <see cref="LatticeMutation.AtomicBatchIndex"/> onto the per-key
/// mutations emitted by an in-flight atomic transaction (a
/// <c>SetManyAtomicAsync</c> saga).
/// </summary>
/// <remarks>
/// <para>
/// Atomic-batch metadata flows on the inbound write path via an Orleans
/// <see cref="RequestContext"/> entry keyed <c>"ol.batch"</c>. The
/// <c>AtomicWriteGrain</c> coordinator captures the batch size once on
/// the first <c>Prepare</c>, persists it on its grain state, and
/// re-stamps a <c>(Size, Index)</c> pair onto this ambient at the head
/// of every per-key call it issues (including compensation rolls) so
/// the leaf grain mutation publish helpers can read the pair and
/// stamp the corresponding wire slots. Single-key writes outside a
/// saga leave the context unset; the publish helpers default both
/// slots to <c>0</c>.
/// </para>
/// <para>
/// The library itself does not interpret the pair beyond carrying it
/// through to <see cref="LatticeMutation"/>; replication consumers
/// read the slots to reconstruct atomic-batch sibling membership for
/// receiver-side staging.
/// </para>
/// </remarks>
public static class LatticeAtomicBatchContext
{
    /// <summary>
    /// Gets or sets the atomic-batch <c>(Size, Index)</c> pair on the
    /// ambient <see cref="RequestContext"/>. Setting <see langword="null"/>
    /// removes the key rather than storing a null value, matching the
    /// "not in a saga" default.
    /// </summary>
    public static (int Size, int Index)? Current
    {
        get => RequestContext.Get(LatticeEventConstants.AtomicBatchRequestContextKey)
            is ValueTuple<int, int> pair
            ? (pair.Item1, pair.Item2)
            : null;
        set
        {
            if (value is null)
            {
                RequestContext.Remove(LatticeEventConstants.AtomicBatchRequestContextKey);
            }
            else
            {
                RequestContext.Set(
                    LatticeEventConstants.AtomicBatchRequestContextKey,
                    (value.Value.Size, value.Value.Index));
            }
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/> to <paramref name="batch"/> for the
    /// lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent.
    /// </summary>
    /// <param name="batch">
    /// The atomic-batch <c>(Size, Index)</c> pair to stamp onto
    /// mutations authored inside the scope, or <see langword="null"/>
    /// to explicitly clear the ambient context.
    /// </param>
    public static IDisposable With((int Size, int Index)? batch)
    {
        var previous = Current;
        Current = batch;
        return new Scope(previous);
    }

    private sealed class Scope((int Size, int Index)? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Current = previous;
        }
    }
}