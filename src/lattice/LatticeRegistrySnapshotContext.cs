using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient per-scan snapshot of <see cref="ITxRegistryGrain"/>
/// decisions, propagated via Orleans <see cref="RequestContext"/> from a
/// lattice-level read fan-out (e.g. <c>GetManyAsync</c>) into every
/// per-shard leaf participating in the scan.
/// </summary>
/// <remarks>
/// <para>
/// The TxRegistry is the single tree-wide linearization point for
/// atomic-write saga commit/abort decisions. A naive read fan-out where
/// each leaf independently calls <c>GetStatusManyAsync</c> on the
/// registry is non-linearizable across the scan: the registry's
/// <see cref="TxStatus.InFlight"/>→<see cref="TxStatus.Committed"/>
/// transition can fall mid-fan-out, so leaf A returns the pre-saga
/// value (status InFlight at the time of its query) while leaf B
/// returns the prepared (post-saga) value (status Committed at the
/// time of its query) - the same split observation strict per-tree
/// atomic visibility is meant to prevent.
/// </para>
/// <para>
/// The fix: the lattice-level scan entry point makes one
/// <see cref="ITxRegistryGrain.SnapshotAsync"/> call before fan-out and
/// stamps the result onto this ambient. Every leaf in the scan reads
/// the stamped snapshot in place of its own registry RPC, so all
/// leaves share a single decision view and the scan is linearizable
/// against the registry's transition moment. Decisions not present in
/// the snapshot default to <see cref="TxStatus.InFlight"/> at the
/// caller - consistent with "decision not yet recorded as of this
/// snapshot's wall-clock moment".
/// </para>
/// <para>
/// Single-key reads do not need the ambient: only one leaf is
/// consulted, so the registry RPC is itself the scan's linearization
/// point. Writes never read the ambient.
/// </para>
/// </remarks>
internal static class LatticeRegistrySnapshotContext
{
    /// <summary>
    /// Gets or sets the ambient per-scan registry snapshot. Setting
    /// <see langword="null"/> removes the key rather than storing a
    /// null value, matching the "no ambient set" default.
    /// </summary>
    public static Dictionary<Guid, TxStatus>? Current
    {
        get => RequestContext.Get(LatticeEventConstants.RegistrySnapshotRequestContextKey)
            as Dictionary<Guid, TxStatus>;
        set
        {
            if (value is null)
            {
                RequestContext.Remove(LatticeEventConstants.RegistrySnapshotRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.RegistrySnapshotRequestContextKey, value);
            }
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/> to <paramref name="snapshot"/> for
    /// the lifetime of the returned scope, restoring the prior value
    /// on <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent.
    /// </summary>
    public static IDisposable BeginScope(Dictionary<Guid, TxStatus>? snapshot)
    {
        var prior = Current;
        Current = snapshot;
        return new Scope(prior);
    }

    private sealed class Scope(Dictionary<Guid, TxStatus>? prior) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            Current = prior;
        }
    }
}
