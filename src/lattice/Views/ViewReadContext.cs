using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Views;

/// <summary>
/// Internal ambient capability that marks the current turn as an authorised
/// reader of materialised-view (<c>view-*</c>) trees.
/// </summary>
/// <remarks>
/// <para>
/// A materialised view tree is derived state owned by its
/// <see cref="IViewMaintainerGrain"/>, and the <em>active generation</em> can be
/// swapped underneath a raw bind by a shadow-swap rebuild (see
/// <c>ViewMaintainerGrain.ShadowSwap</c>); a direct user read of a fixed
/// <c>view-{name}</c> id may therefore observe a stale or empty generation. The
/// public <see cref="ILattice"/> content-read surface rejects reads of any
/// <c>view-*</c> tree unless this capability - or the maintainer's
/// <see cref="ViewWriteContext"/> write capability - is active on the ambient
/// <see cref="RequestContext"/>.
/// </para>
/// <para>
/// The <see cref="ILatticeView"/> read handle opens a <see cref="BeginScope"/>
/// <c>using</c> block around every read it delegates to the active-generation
/// tree, and the maintainer opens one around its own view-tree reads (e.g.
/// digest computation). The flag is stamped on the
/// <see cref="LatticeEventConstants.ViewReadRequestContextKey"/>
/// <see cref="RequestContext"/> entry and propagates across grain calls because
/// <c>RequestContext</c> flows automatically on outgoing calls, so a single scope
/// at the read handle's entry authorises every nested view-tree read. The default
/// outside any scope is unauthorised, so user calls - which never open the scope -
/// are rejected and steered to the read handle.
/// </para>
/// </remarks>
internal static class ViewReadContext
{
    /// <summary>
    /// Gets a value indicating whether an authorised view-read scope is active on
    /// the ambient <see cref="RequestContext"/>.
    /// </summary>
    public static bool IsAuthorised =>
        RequestContext.Get(LatticeEventConstants.ViewReadRequestContextKey) is bool active && active;

    /// <summary>
    /// Marks the ambient context as an authorised view-read scope for the lifetime
    /// of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is idempotent.
    /// </summary>
    public static IDisposable BeginScope()
    {
        var previous = RequestContext.Get(LatticeEventConstants.ViewReadRequestContextKey) as bool?;
        RequestContext.Set(LatticeEventConstants.ViewReadRequestContextKey, true);
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
                RequestContext.Remove(LatticeEventConstants.ViewReadRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.ViewReadRequestContextKey, previous.Value);
            }
        }
    }
}
