using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Startup guard that fails fast when the replication configuration is internally
/// inconsistent in a way that would otherwise only surface as a runtime fault on
/// the first write. It asserts the flag-CRDT membership invariant for every
/// statically declared tree by routing each <c>(treeId, mode)</c> pair through
/// the reusable <see cref="ILatticeReplicationPreconditionValidator"/>: a tree
/// declared under a flag merge mode (<see cref="LatticeMergeMode.OrFlag"/> or
/// <see cref="LatticeMergeMode.RwFlag"/>) needs a non-empty
/// <see cref="ILatticeReplicationContext.LocalReplicaId"/> to author its dots, so
/// declaring such a tree without a configured
/// <see cref="LatticeReplicationOptions.ClusterId"/> is rejected at silo start
/// rather than when a feature first tries to write to it.
/// <para>
/// The guard validates the same static options seam features consume, and shares
/// its precondition logic with the runtime enable path (which calls the same
/// validator on a per-request basis), so a statically declared tree and a
/// runtime-enabled tree are held to an identical safety bar.
/// </para>
/// </summary>
internal sealed class LatticeReplicationMergeModeStartupValidator(
    ILatticeReplicationPreconditionValidator preconditionValidator,
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
            var result = preconditionValidator.Validate(kvp.Key, kvp.Value);
            if (!result.IsSatisfied)
            {
                throw new InvalidOperationException(result.FailureReason);
            }
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
