using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient scope that carries a server-side predicate IR
/// (<see cref="LatticePredicateNode"/>) on the Orleans
/// <see cref="RequestContext"/> so it flows from a typed predicate overload on
/// the extension layer down into the owning leaf's read / scan /
/// conditional-mutation path. The leaf evaluates the IR against each candidate
/// value's JSON document view (see the internal predicate evaluator) and drops
/// non-matching values before they are paged or returned, so the filter is
/// applied server-side and non-matching values never cross the wire.
/// <para>
/// The scope adds zero ambient cost when it is not entered: every operation
/// reads <see cref="IsActive"/> (a single <see cref="RequestContext"/> probe)
/// and takes its existing un-predicated path when no predicate is set. The
/// typed extension overloads open the scope for the duration of the single
/// grain call they make; durable cursors persist the IR on
/// <see cref="LatticeCursorSpec"/> instead, because an ambient scope does not
/// survive silo failover.
/// </para>
/// </summary>
public static class LatticePredicateContext
{
    /// <summary>
    /// <c>true</c> when a predicate is currently set on the ambient
    /// <see cref="RequestContext"/>.
    /// </summary>
    public static bool IsActive =>
        RequestContext.Get(LatticeEventConstants.PredicateRequestContextKey) is LatticePredicateNode;

    /// <summary>
    /// Gets or sets the predicate IR on the ambient
    /// <see cref="RequestContext"/>. Setting <c>null</c> removes the entry
    /// rather than storing a null, matching the "no predicate" default.
    /// </summary>
    public static LatticePredicateNode? Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.PredicateRequestContextKey);
            return raw is LatticePredicateNode node ? node : null;
        }
        set
        {
            if (value is null)
                RequestContext.Remove(LatticeEventConstants.PredicateRequestContextKey);
            else
                RequestContext.Set(LatticeEventConstants.PredicateRequestContextKey, value.Value);
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/> to <paramref name="predicate"/> for the
    /// lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is idempotent.
    /// </summary>
    public static IDisposable With(LatticePredicateNode? predicate)
    {
        var previous = Current;
        Current = predicate;
        return new Scope(previous);
    }

    private sealed class Scope(LatticePredicateNode? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            Current = previous;
        }
    }
}
