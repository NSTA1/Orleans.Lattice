using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The cross-tree preconditions on serving a snapshot export to a peer (issue
/// #4684). While any silo of this cluster predates the cross-tree decision
/// purge hold, that silo can purge a cross-tree sub-saga's decision with no
/// regard for the peers of its sibling trees, so an export could carry a
/// participant as bare committed rows that a receiver's barrier never learns
/// of. Every export is therefore deferred - a transient refusal the receiver
/// retries - until every silo honours the hold. The same check keeps
/// <see cref="ReplicationCrossTreeDecisionHold"/> from releasing anything
/// meanwhile.
/// </summary>
internal sealed class CrossTreeExportGate(IServiceProvider services)
{
    /// <summary>Test seam: overrides the cluster-manifest check.</summary>
    internal Func<bool>? AllSilosHonourOverrideForTesting { get; set; }

    /// <summary>Test seam: restricts the siblings a capture lists, beyond the replicated trees.</summary>
    internal Func<string, bool>? SiblingFilterForTesting { get; set; }

    /// <summary>
    /// Captures, for every tree this cluster replicates other than
    /// <paramref name="treeName"/>, its physical write-ahead log, each
    /// partition's next sequence and its export epoch (issue #4684). Called at
    /// the end of an export, so every sibling record the export's rows can
    /// depend on lies below the captured tails. A receiver keeps the imported
    /// tree read-fenced until each sibling it replicates has passed its
    /// boundary.
    /// </summary>
    public async Task<ImmutableDictionary<string, CrossTreeSiblingBoundary>> CaptureSiblingBoundariesAsync(
        string treeName, ILatticeReplicationContext replicationContext, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentNullException.ThrowIfNull(replicationContext);
        var grainFactory = services.GetRequiredService<IGrainFactory>();
        var options = services.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();
        var registry = grainFactory.GetLatticeRegistry();
        var builder = ImmutableDictionary.CreateBuilder<string, CrossTreeSiblingBoundary>(StringComparer.Ordinal);
        foreach (var sibling in await registry.GetAllTreeIdsAsync().ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (string.Equals(sibling, treeName, StringComparison.Ordinal)
                || replicationContext.ResolveMergeMode(sibling) is null
                || SiblingFilterForTesting?.Invoke(sibling) == false)
            {
                continue;
            }

            // The epoch first: an import of the sibling numbered above it opened
            // after this capture.
            var epoch = await grainFactory.GetGrain<IReplicationExportEpochGrain>(sibling).GetAsync().ConfigureAwait(false);
            var physical = (await registry.GetEntryAsync(sibling).ConfigureAwait(false))?.PhysicalTreeId ?? sibling;
            var partitions = Math.Max(1, options.Get(sibling).ReplogPartitions);
            var tails = new Task<long>[partitions];
            for (var p = 0; p < partitions; p++)
            {
                tails[p] = grainFactory.GetGrain<IWalShardGrain>($"{physical}/{p}").GetNextSequenceAsync(cancellationToken).AsTask();
            }

            builder[sibling] = new CrossTreeSiblingBoundary
            {
                PhysicalTreeId = physical,
                Tails = [.. await Task.WhenAll(tails).ConfigureAwait(false)],
                ExportEpoch = epoch,
            };
        }

        return builder.ToImmutable();
    }

    /// <summary>
    /// Throws <see cref="LatticeSnapshotExportDeferredException"/> while the
    /// export of <paramref name="treeName"/> must wait.
    /// </summary>
    public void EnsureMayExport(string treeName)
    {
        if (!AllSilosHonourCrossTreeHold())
        {
            ThrowDeferred(treeName);
        }
    }

    /// <summary>
    /// Whether every silo of this cluster honours the cross-tree decision purge
    /// hold. The hold itself releases nothing until it does.
    /// </summary>
    public bool AllSilosHonourCrossTreeHold() =>
        AllSilosHonourOverrideForTesting?.Invoke() ?? PurgeHoldSupport.AllSilosHonourCrossTreeHold(services);

    [DoesNotReturn]
    private static void ThrowDeferred(string treeName) =>
        throw new LatticeSnapshotExportDeferredException(
            $"Snapshot export of tree '{treeName}' is deferred: a silo of this cluster predates the cross-tree "
            + "decision purge hold, so an export could omit a cross-tree sub-saga's decision. Retry once every "
            + "silo is upgraded.");
}
