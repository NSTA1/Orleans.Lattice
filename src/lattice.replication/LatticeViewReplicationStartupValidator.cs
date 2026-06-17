using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Startup guard that fails fast when a materialised view's
/// <see cref="LatticeViewOptions.ReplicationMode"/> is inconsistent with the
/// replication configuration in a way that would otherwise only surface as a
/// silent correctness fault (two writers, or a view consumers never receive).
/// Mirrors <see cref="LatticeReplicationMergeModeStartupValidator"/>: it runs as
/// an <see cref="IHostedService"/> and throws <see cref="InvalidOperationException"/>
/// from <see cref="StartAsync"/> so the silo refuses to start.
/// <para>
/// For each startup-declared view it resolves the per-view
/// <see cref="LatticeViewOptions.ReplicationMode"/>, computes the stable view tree
/// id <c>view-{viewName}</c>, and reads the replicated-trees map. It rejects two
/// misconfigurations:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <see cref="LatticeViewReplicationMode.DeriveLocally"/> with the view tree (or
/// any of its generation-suffixed family, <c>view-{name}#g*</c>) present in the
/// replicated-trees map: the maintainer runs on every cluster <i>and</i> the tree
/// is replicated in, so two writers would race on the same tree.
/// </description></item>
/// <item><description>
/// <see cref="LatticeViewReplicationMode.ShipView"/> with the view tree absent
/// from the replicated-trees map: the maintainer runs only on the producer and the
/// tree is never shipped, so consumer clusters would never receive the view.
/// </description></item>
/// </list>
/// </summary>
internal sealed class LatticeViewReplicationStartupValidator(
    IServiceProvider services,
    IOptionsMonitor<LatticeViewOptions> viewOptions,
    IOptionsMonitor<LatticeReplicationOptions> replicationOptions) : IHostedService
{
    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var registrations = services.GetService<IReadOnlyList<StartupViewRegistration>>();
        if (registrations is null || registrations.Count == 0)
        {
            return Task.CompletedTask;
        }

        var trees = replicationOptions.CurrentValue.ReplicatedTrees;

        foreach (var registration in registrations)
        {
            var mode = viewOptions.Get(registration.ViewName).ReplicationMode;
            var viewTreeId = $"view-{registration.ViewName}";

            if (mode == LatticeViewReplicationMode.DeriveLocally)
            {
                var conflicting = FindDeriveLocallyConflict(trees, viewTreeId);
                if (conflicting is not null)
                {
                    throw new InvalidOperationException(
                        $"View '{registration.ViewName}' uses {nameof(LatticeViewReplicationMode)}."
                        + $"{nameof(LatticeViewReplicationMode.DeriveLocally)} but its view tree '{conflicting}' is declared in "
                        + $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ReplicatedTrees)}. "
                        + $"{nameof(LatticeViewReplicationMode.DeriveLocally)} runs the maintainer on every cluster, so "
                        + "replicating the view tree in as well would create two writers on the same tree. Either remove "
                        + "the view tree from the replicated-trees map, or switch the view to "
                        + $"{nameof(LatticeViewReplicationMode)}.{nameof(LatticeViewReplicationMode.ShipView)} so the maintainer "
                        + "runs only on the producer cluster(s).");
                }
            }
            else if (trees is null || !trees.ContainsKey(viewTreeId))
            {
                throw new InvalidOperationException(
                    $"View '{registration.ViewName}' uses {nameof(LatticeViewReplicationMode)}."
                    + $"{nameof(LatticeViewReplicationMode.ShipView)} but its view tree '{viewTreeId}' is not declared in "
                    + $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ReplicatedTrees)}. "
                    + $"{nameof(LatticeViewReplicationMode.ShipView)} suppresses the maintainer on consumer clusters and ships "
                    + "the view tree to them, so the tree must be replicated or consumer clusters would never receive the view. "
                    + "Either add the view tree to the replicated-trees map, or switch the view to "
                    + $"{nameof(LatticeViewReplicationMode)}.{nameof(LatticeViewReplicationMode.DeriveLocally)} so every cluster "
                    + "derives the view from its local source.");
            }
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Returns the offending replicated-tree key when a
    /// <see cref="LatticeViewReplicationMode.DeriveLocally"/> view's stable tree id
    /// or any of its generation-suffixed family (<c>view-{name}#g*</c>) is present
    /// in the map, otherwise <see langword="null"/>.
    /// </summary>
    private static string? FindDeriveLocallyConflict(
        IReadOnlyDictionary<string, LatticeMergeMode>? trees,
        string viewTreeId)
    {
        if (trees is null)
        {
            return null;
        }

        if (trees.ContainsKey(viewTreeId))
        {
            return viewTreeId;
        }

        var generationPrefix = $"{viewTreeId}#g";
        foreach (var key in trees.Keys)
        {
            if (key.StartsWith(generationPrefix, StringComparison.Ordinal))
            {
                return key;
            }
        }

        return null;
    }
}
