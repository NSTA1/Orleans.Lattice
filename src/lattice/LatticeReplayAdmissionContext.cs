using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// The admission class a call chain claims when one of its grain calls has to
/// queue for a WAL replay permit. Absent means
/// <see cref="LatticeReplayAdmissionClass.Interactive"/>.
/// </summary>
public enum LatticeReplayAdmissionClass
{
    /// <summary>
    /// A latency-sensitive call whose cost is bounded by one request. The
    /// default for every caller that does not opt in, because a caller that has
    /// never heard of this seam must never be the one that gets de-prioritised.
    /// </summary>
    Interactive = 0,

    /// <summary>
    /// Background work whose cost scales with the corpus rather than with one
    /// request - an index key walk, a re-derivation sweep, a bulk re-embed.
    /// Admitted to the replay-permit queue at a shallower depth than
    /// <see cref="Interactive"/>, so a bulk fan-out cannot fill the queue an
    /// interactive read has to enter.
    /// </summary>
    Bulk = 1,
}

/// <summary>
/// Declares the ambient WAL replay admission class for a call chain, so the
/// per-silo replay permit queue can be bounded <b>and prioritised</b> rather
/// than merely bounded.
/// <para>
/// <b>Why a class is needed at all (issue #3284).</b> The replay permit pool is
/// a shared, scarce, per-silo resource. Nothing used to admit work against it,
/// so a bulk <c>O(corpus)</c> walk and a single interactive read contended as
/// equals and an unbounded number of waiters was admissible: 87 waiters were
/// measured against a ceiling of 6, which is not a queue but a backlog that
/// guarantees timeouts. Bounding the queue alone would refuse the interactive
/// read as readily as the walk that filled it, so the bound has to know which
/// is which.
/// </para>
/// <para>
/// <b>The flag flows, and that is the whole mechanism.</b> It is carried on an
/// Orleans <see cref="RequestContext"/> entry, which propagates automatically
/// across grain calls, so a caller marks the top of its own fan-out once and
/// every leaf activation the fan-out provokes is classified without any of the
/// intervening code knowing this type exists.
/// </para>
/// <para>
/// <b>It is not a priority boost.</b> Declaring a scope can only ever make the
/// caller <i>more</i> likely to be refused admission; there is no value of this
/// flag that admits a caller the interactive bound would have refused. That
/// asymmetry is deliberate: a seam that could raise a caller's priority would be
/// a seam every caller eventually sets.
/// </para>
/// </summary>
public static class LatticeReplayAdmissionContext
{
    /// <summary>
    /// The ambient admission class. Returns
    /// <see cref="LatticeReplayAdmissionClass.Interactive"/> outside any scope.
    /// </summary>
    public static LatticeReplayAdmissionClass Current =>
        RequestContext.Get(LatticeEventConstants.ReplayAdmissionBulkRequestContextKey) is bool bulk && bulk
            ? LatticeReplayAdmissionClass.Bulk
            : LatticeReplayAdmissionClass.Interactive;

    /// <summary>
    /// Marks the ambient context as <see cref="LatticeReplayAdmissionClass.Bulk"/>
    /// for the lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is idempotent.
    /// </summary>
    /// <returns>The scope to dispose when the bulk work ends.</returns>
    public static IDisposable BeginBulkScope()
    {
        var previous =
            RequestContext.Get(LatticeEventConstants.ReplayAdmissionBulkRequestContextKey) as bool?;
        RequestContext.Set(LatticeEventConstants.ReplayAdmissionBulkRequestContextKey, true);
        return new Scope(previous);
    }

    private sealed class Scope(bool? previous) : IDisposable
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
                RequestContext.Remove(LatticeEventConstants.ReplayAdmissionBulkRequestContextKey);
            }
            else
            {
                RequestContext.Set(
                    LatticeEventConstants.ReplayAdmissionBulkRequestContextKey, previous.Value);
            }
        }
    }
}
