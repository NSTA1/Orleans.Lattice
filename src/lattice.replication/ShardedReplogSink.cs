using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplogSink"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// Routes each captured <see cref="ReplogEntry"/> to a single
/// <see cref="IReplogShardGrain"/> activation keyed by
/// <c>{treeId}/{partition}</c>, where <c>partition</c> is a stable hash
/// of the entry's key modulo
/// <see cref="LatticeReplicationOptions.ReplogPartitions"/>.
/// <para>
/// The WAL append is awaited inline so a failure surfaces to the
/// originating writer rather than being silently swallowed in a
/// best-effort post-write append. Replaces the legacy
/// <c>NoOpReplogSink</c> as the default once the WAL grain is wired
/// into the pipeline.
/// </para>
/// <para>
/// The partition count is read from the unnamed (default) options
/// instance via <see cref="IOptionsMonitor{TOptions}.CurrentValue"/>.
/// Per-tree partition-count overrides are not supported - if a future
/// phase needs them, the resolution path will be widened to include the
/// tree-id named instance.
/// </para>
/// </summary>
internal sealed class ShardedReplogSink(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options) : IReplogSink
{
    /// <inheritdoc />
    public Task WriteAsync(ReplogEntry entry, CancellationToken cancellationToken)
    {
        var partitions = options.CurrentValue.ReplogPartitions;
        var partition = ReplogPartitionHash.Compute(entry.Key ?? string.Empty, partitions);
        var grain = grainFactory.GetGrain<IReplogShardGrain>($"{entry.TreeId}/{partition}");
        return grain.AppendAsync(entry, cancellationToken);
    }
}
