using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient maintenance-context flag used to stamp
/// <see cref="LatticeMutation.Category"/> as
/// <see cref="MutationCategory.Maintenance"/> for the duration of a
/// library-internal structural mutation (resize / rebalance / compaction /
/// internal rewrite). Public callers do not interact with this type —
/// observers read <see cref="LatticeMutation.Category"/> directly.
/// </summary>
/// <remarks>
/// <para>
/// The flag flows on the inbound write path via an Orleans
/// <see cref="RequestContext"/> entry keyed
/// <see cref="LatticeEventConstants.MaintenanceRequestContextKey"/>.
/// Internal mutation sites wrap their work in a
/// <see cref="BeginScope"/> <c>using</c> block; emits made inside the
/// scope are stamped with <see cref="MutationCategory.Maintenance"/>,
/// while emits outside the scope default to
/// <see cref="MutationCategory.User"/>. The flag is preserved across
/// grain calls because <see cref="RequestContext"/> propagates
/// automatically.
/// </para>
/// <para>
/// The default outside any scope is <see cref="MutationCategory.User"/>,
/// matching the documented "wire-compatible default" semantics for
/// observers persisted before this field existed.
/// </para>
/// </remarks>
internal static class LatticeMaintenanceContext
{
    /// <summary>
    /// Gets the current ambient mutation category. Returns
    /// <see cref="MutationCategory.Maintenance"/> when a maintenance scope
    /// is active on the <see cref="RequestContext"/>; otherwise returns
    /// <see cref="MutationCategory.User"/>.
    /// </summary>
    public static MutationCategory Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.MaintenanceRequestContextKey);
            return raw is bool active && active ? MutationCategory.Maintenance : MutationCategory.User;
        }
    }

    /// <summary>
    /// Marks the ambient context as a maintenance scope for the lifetime
    /// of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent.
    /// </summary>
    public static IDisposable BeginScope()
    {
        var previous = RequestContext.Get(LatticeEventConstants.MaintenanceRequestContextKey) as bool?;
        RequestContext.Set(LatticeEventConstants.MaintenanceRequestContextKey, true);
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
                RequestContext.Remove(LatticeEventConstants.MaintenanceRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.MaintenanceRequestContextKey, previous.Value);
            }
        }
    }
}
