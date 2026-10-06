using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// The one place a mutation that changes which trees exist, or how one is laid
/// out, says so: a reshard, resize, snapshot, restore, alias, delete, recover or
/// purge. It forgets every per-circuit list of trees, so the next read of the
/// Cluster area's catalogue (its tree list, shard counts, badge and completions)
/// and of the Data area's directory (its tree list and tree pickers) goes to the
/// cluster rather than showing what the circuit read before the change.
/// </summary>
/// <remarks>
/// The lists are resolved optionally, so a head that registers only one area
/// still forgets the one it has.
/// </remarks>
/// <param name="services">The circuit's services.</param>
internal sealed class ClusterTreeChanges(IServiceProvider services)
{
    /// <summary>Forgets every remembered list of trees.</summary>
    public void Changed()
    {
        services.GetService<ClusterTreeCatalog>()?.Invalidate();
        services.GetService<DataDirectory>()?.Invalidate();
    }
}
