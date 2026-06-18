using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Views;

/// <summary>
/// Internal ambient capability that marks the current turn as a
/// maintainer-authorised writer of materialised-view (<c>view-*</c>) trees.
/// </summary>
/// <remarks>
/// <para>
/// A materialised view tree is derived state owned by its
/// <see cref="IViewMaintainerGrain"/>; a direct user write would corrupt the
/// view and trigger a spurious rebuild. The public <see cref="ILattice"/> write
/// surface therefore rejects writes to any <c>view-*</c> tree unless this
/// capability is active on the ambient <see cref="RequestContext"/>.
/// </para>
/// <para>
/// The view maintainer and the cross-tree coordinator open a
/// <see cref="BeginScope"/> <c>using</c> block around any turn that writes to a
/// view tree. The flag is stamped on the
/// <see cref="LatticeEventConstants.ViewWriteRequestContextKey"/>
/// <see cref="RequestContext"/> entry and propagates across grain calls
/// (including the cross-tree atomic-write saga) because <c>RequestContext</c>
/// flows automatically on outgoing calls, so a single scope at the maintainer's
/// turn entry authorises every nested view-tree write. The default outside any
/// scope is unauthorised, so user calls - which never open the scope - are
/// rejected.
/// </para>
/// </remarks>
internal static class ViewWriteContext
{
    /// <summary>
    /// Gets a value indicating whether a maintainer view-write scope is active on
    /// the ambient <see cref="RequestContext"/>.
    /// </summary>
    public static bool IsAuthorised =>
        RequestContext.Get(LatticeEventConstants.ViewWriteRequestContextKey) is bool active && active;

    /// <summary>
    /// Marks the ambient context as a maintainer-authorised view-write scope for
    /// the lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is idempotent.
    /// </summary>
    public static IDisposable BeginScope()
    {
        var previous = RequestContext.Get(LatticeEventConstants.ViewWriteRequestContextKey) as bool?;
        RequestContext.Set(LatticeEventConstants.ViewWriteRequestContextKey, true);
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
                RequestContext.Remove(LatticeEventConstants.ViewWriteRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.ViewWriteRequestContextKey, previous.Value);
            }
        }
    }
}
