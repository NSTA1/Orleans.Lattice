namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Ambient carrier and timing for the saga decision gate a snapshot capture
/// holds (issue #4485).
/// <para>
/// A single-tree snapshot open acquires and releases its own gate. A
/// cross-tree-consistent backup set instead holds one gate token across every
/// member tree for the whole set capture and opens each member's snapshot with
/// that token on the ambient request context (<see cref="With"/>); the open then
/// resolves against the set's gate and neither acquires nor releases one.
/// </para>
/// </summary>
internal static class SnapshotDecisionGateContext
{
    /// <summary>The request-context key carrying an externally held gate token.</summary>
    internal const string RequestContextKey = "orleans.lattice.snapshot.decisionGate";

    /// <summary>The lease a capture's gate is acquired and renewed with.</summary>
    internal static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);

    /// <summary>How often a capture renews its gate while it holds it.</summary>
    internal static readonly TimeSpan RenewInterval = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How many times a snapshot open retries a capture whose own gate lapsed
    /// before it gives up with <see cref="LatticeTransactionOutcomeUnavailableException"/>.
    /// </summary>
    internal const int MaxCaptureAttempts = 3;

    /// <summary>
    /// The externally held gate token on the current request context, or
    /// <see langword="null"/> when the caller holds none.
    /// </summary>
    internal static Guid? Current =>
        Orleans.Runtime.RequestContext.Get(RequestContextKey) is Guid token && token != Guid.Empty
            ? token
            : null;

    /// <summary>
    /// Places <paramref name="token"/> on the request context until the returned
    /// scope is disposed, so every snapshot opened in the scope resolves against
    /// that externally held gate.
    /// </summary>
    /// <param name="token">The externally held gate token.</param>
    /// <returns>A scope that restores the previous value on dispose.</returns>
    internal static IDisposable With(Guid token)
    {
        var previous = Orleans.Runtime.RequestContext.Get(RequestContextKey);
        Orleans.Runtime.RequestContext.Set(RequestContextKey, token);
        return new Scope(previous);
    }

    private sealed class Scope(object? previous) : IDisposable
    {
        public void Dispose()
        {
            if (previous is null)
                Orleans.Runtime.RequestContext.Remove(RequestContextKey);
            else
                Orleans.Runtime.RequestContext.Set(RequestContextKey, previous);
        }
    }
}
