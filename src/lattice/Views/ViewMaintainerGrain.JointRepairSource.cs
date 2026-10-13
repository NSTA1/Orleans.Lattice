using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Views;

internal sealed partial class ViewMaintainerGrain
{
    private Task<RoutingInfo> ReadJointSourceRoutingAsync(ViewCrossTreeRepairHold hold)
        => ReadPhysicalSourceRoutingAsync(hold.SourceTreeId, hold.WalTreeId);

    private async Task<Dictionary<string, T>> ReadJointSourceAsync<T>(
        ViewCrossTreeRepairHold hold,
        List<string> keys,
        Func<IShardRootGrain, List<string>, Task<List<T>>> read,
        CancellationToken cancellationToken)
    {
        // A maintainer reads the retained physical roots directly, even when
        // its caller carries a logical routing marker from a different turn.
        var previous = RequestContext.Get(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey);
        RequestContext.Remove(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey);
        try
        {
            for (var attempt = 1; ; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var routing = await ReadJointSourceRoutingAsync(hold);
                try
                {
                    return await ReadSourceOnceAsync(routing, keys, read, cancellationToken);
                }
                catch (Exception ex) when ((ex is StaleShardRoutingException or StaleTreeRoutingException)
                    && attempt < SourceVersionRoutingAttempts)
                {
                    // Refresh only this physical tree's authoritative map.
                    // Never follow the logical alias to a replacement copy.
                }
            }
        }
        finally
        {
            if (previous is null)
            {
                RequestContext.Remove(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey, previous);
            }
        }
    }
}
