using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Startup guard that fails fast when the replication configuration is internally
/// inconsistent in a way that would otherwise only surface as a runtime fault on
/// the first write. Today it asserts the flag-CRDT membership invariant: a tree
/// declared under a flag merge mode (<see cref="LatticeMergeMode.OrFlag"/> or
/// <see cref="LatticeMergeMode.RwFlag"/>) needs a non-empty
/// <see cref="ILatticeReplicationContext.LocalReplicaId"/> to author its dots, so
/// declaring such a tree without a configured
/// <see cref="LatticeReplicationOptions.ClusterId"/> is rejected at silo start
/// rather than when a feature (for example a flag-membership tag index) first
/// tries to write to it.
/// <para>
/// The guard runs against the replication-configuration seam, so it validates
/// exactly the view features consume - not a parallel copy of the options.
/// </para>
/// </summary>
internal sealed class LatticeReplicationMergeModeStartupValidator(
    ILatticeReplicationContext replicationContext,
    IOptionsMonitor<LatticeReplicationOptions> options) : IHostedService
{
    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var trees = options.CurrentValue.ReplicatedTrees;
        if (trees is null)
        {
            return Task.CompletedTask;
        }

        foreach (var kvp in trees)
        {
            var isFlagMode = kvp.Value is LatticeMergeMode.OrFlag or LatticeMergeMode.RwFlag;
            if (isFlagMode && string.IsNullOrEmpty(replicationContext.LocalReplicaId))
            {
                throw new InvalidOperationException(
                    $"Tree '{kvp.Key}' is declared with the flag merge mode '{kvp.Value}' in "
                    + $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ReplicatedTrees)}, "
                    + "but no local replica id is configured. Flag-CRDT membership authors its "
                    + "enable/disable dots with the local replica id, so "
                    + $"{nameof(LatticeReplicationOptions)}.{nameof(LatticeReplicationOptions.ClusterId)} "
                    + "must be set to a non-empty, globally-unique cluster identifier. Set it, or "
                    + "remove the flag merge mode for this tree if it does not need multi-writer "
                    + "convergence.");
            }
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
