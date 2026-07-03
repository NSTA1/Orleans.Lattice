using Orleans.Runtime;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Ambient, opt-in seam that lets a caller demand a minimum locally-compiled
/// policy epoch before a user write to a strict-consistency tree is accepted
/// (issue #982). It is the client-visible half of the optional strict-consistency
/// fence: the tree is opted in on
/// <see cref="LatticeAuthOptions.StrictConsistencyTrees"/>, and the required
/// floor for a given unit of work is supplied here.
/// </summary>
/// <remarks>
/// <para>
/// The floor is stamped on the ambient <see cref="RequestContext"/>, so it flows
/// automatically across grain calls (and across an outgoing cluster hop) for the
/// duration of the returned scope, exactly like the other ambient markers in the
/// codebase. Outside any scope <see cref="RequiredEpoch"/> is <c>null</c> and the
/// enforcement gate skips the fence entirely, which is the default eventual path.
/// </para>
/// <para>
/// Typical use: a client that has just observed policy epoch <c>N</c> on one site
/// (for example after authoring a revoke there) opens
/// <c>RequireAtLeast(N)</c> around subsequent writes it routes to other sites, so
/// a site whose replicated policy has not yet reached epoch <c>N</c> rejects the
/// write rather than authorizing it against stale policy. The fence only ever
/// affects <b>user</b> writes to a strict-configured tree; reads and
/// system-origin / replication-applied writes are never fenced.
/// </para>
/// </remarks>
public static class LatticePolicyEpochFenceContext
{
    /// <summary>
    /// The <see cref="RequestContext"/> key under which the required policy-epoch
    /// floor is stored. Prefixed like the auth package's other ambient keys.
    /// </summary>
    private const string RequiredEpochKey = "olz.policy-epoch-floor";

    /// <summary>
    /// Gets the required minimum policy epoch demanded by the innermost active
    /// <see cref="RequireAtLeast"/> scope, or <c>null</c> when no floor is
    /// required on the current turn (the default eventual behaviour).
    /// </summary>
    public static long? RequiredEpoch =>
        RequestContext.Get(RequiredEpochKey) is long value ? value : null;

    /// <summary>
    /// Requires that the locally-compiled policy epoch be at least
    /// <paramref name="epoch"/> for the lifetime of the returned scope. A user
    /// write to a strict-consistency tree made while this cluster's epoch is below
    /// the floor is rejected. Nesting never weakens an outer requirement: the
    /// effective floor within a nested scope is the greater of the outer floor and
    /// <paramref name="epoch"/>. The prior floor is restored on
    /// <see cref="IDisposable.Dispose"/>; disposal is idempotent.
    /// </summary>
    /// <param name="epoch">The minimum required policy epoch. Must not be negative.</param>
    /// <returns>A scope that clears (or restores) the floor when disposed.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="epoch"/> is negative.</exception>
    public static IDisposable RequireAtLeast(long epoch)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(epoch);

        var previous = RequestContext.Get(RequiredEpochKey) as long?;
        var effective = previous is long p && p > epoch ? p : epoch;
        RequestContext.Set(RequiredEpochKey, effective);
        return new Scope(previous);
    }

    private sealed class Scope(long? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (previous is null)
            {
                RequestContext.Remove(RequiredEpochKey);
            }
            else
            {
                RequestContext.Set(RequiredEpochKey, previous.Value);
            }
        }
    }
}
