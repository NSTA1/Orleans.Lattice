using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.State;

/// <summary>
/// Resolves the source tree backing a materialised-view (<c>view-*</c>) tree, so
/// every state-API read surface authorises a view against the tree its rows
/// actually come from rather than against the view id the caller supplied.
/// </summary>
/// <remarks>
/// <para>
/// This exists as one shared seam rather than as a private helper on each facade
/// because the rule it implements is load-bearing and was previously enforced in
/// only one of the two places that needs it. A view read binds under a
/// <see cref="ViewReadContext"/> scope that makes the data-plane access gate
/// bypass itself, and the change feed does not flow through that gate at all - so
/// on both surfaces the SOURCE tree's read grant is the only authorization
/// boundary a view read has. A surface that authorises <c>view-orders</c> against
/// <c>view-orders</c> asks a question whose answer cannot protect <c>orders</c>.
/// </para>
/// <para>
/// Fails closed: a view whose name cannot be recovered, or whose source cannot be
/// resolved from either the local catalog or the durable registry, returns
/// <see langword="null"/> and every caller treats that as "hide the view" rather
/// than as "no restriction".
/// </para>
/// </remarks>
internal static class LatticeStateViewSource
{
    /// <summary>
    /// Resolves the source tree id backing <paramref name="treeId"/>. Prefers the
    /// allocation-free local <see cref="IViewCatalog"/> (every startup-declared
    /// view and any runtime view rehydrated on this silo), then falls back to the
    /// durable cluster-wide <see cref="IViewRegistryGrain"/> for a runtime view
    /// created on another silo.
    /// </summary>
    /// <returns>
    /// The source tree id, or <see langword="null"/> when the view name cannot be
    /// recovered or the source cannot be resolved, so the caller fails closed.
    /// </returns>
    public static async ValueTask<string?> ResolveAsync(
        IServiceProvider services,
        IGrainFactory grainFactory,
        string treeId,
        CancellationToken cancellationToken)
    {
        var viewName = LatticeViewTrees.ViewNameFromTreeId(treeId);
        if (viewName.Length == 0)
        {
            return null;
        }

        var local = services.GetService<IViewCatalog>()?.TryGet(viewName);
        if (local is { } registration)
        {
            return registration.SourceTreeId;
        }

        try
        {
            IReadOnlyList<RuntimeViewRegistration> runtime;
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                var registry = grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey);
                runtime = await registry.ListAsync().ConfigureAwait(false);
            }

            foreach (var reg in runtime)
            {
                if (string.Equals(reg.ViewName, viewName, StringComparison.Ordinal))
                {
                    return reg.SourceTreeId;
                }
            }
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            // Fail closed below on any transient registry-activation failure.
        }

        return null;
    }
}
