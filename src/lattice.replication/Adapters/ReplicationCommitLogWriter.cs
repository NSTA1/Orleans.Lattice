using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Adapters;

/// <summary>
/// Default <see cref="ICommitLogWriter"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication(ISiloBuilder, Action{LatticeReplicationOptions})"/>.
/// Translates a core <see cref="LatticeMutation"/> to a wire-shaped
/// <see cref="ReplogEntry"/> via <see cref="ReplogEntryConverter"/> and
/// dispatches it to the per-shard
/// <see cref="IReplogShardGrain.AppendAsync"/> entry point so the
/// caller observes the per-shard sequence number.
/// <para>
/// Bypasses <see cref="IReplogSink"/> by design — the public sink seam
/// returns <see cref="System.Threading.Tasks.Task"/> rather than
/// <see cref="System.Threading.Tasks.Task{Long}"/>, and the leaf
/// commit path needs the assigned offset to drive replay coordination
/// after a leaf reactivation.
/// </para>
/// <para>
/// A complementary short-circuit on
/// <c>ReplicationMutationObserver</c> suppresses double WAL appends
/// from the post-commit observer dispatch when the foreground commit
/// path has already appended the same mutation.
/// </para>
/// </summary>
internal sealed class ReplicationCommitLogWriter(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options,
    IReplicationModeResolver modeResolver) : ICommitLogWriter
{
    /// <inheritdoc />
    public async Task<long> AppendAsync(LatticeMutation mutation, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var perTree = options.Get(mutation.TreeId);
        var partitions = perTree.ReplogPartitions;
        var clusterId = perTree.ClusterId ?? string.Empty;

        // Resolve the declared replication mode for this tree; default
        // to LwwRegister so a hand-constructed test call site still
        // gets a meaningful value on the wire. The foreground commit
        // path only invokes this adapter for trees the resolver
        // accepts, but adding a default here keeps the unit-test
        // surface trivially exercisable without a fully-configured
        // mode-resolver fake.
        var mode = modeResolver.Resolve(mutation.TreeId) ?? ReplicationMode.LwwRegister;

        var entry = ReplogEntryConverter.ToReplogEntry(mutation, mode, clusterId);
        var partition = ReplogPartitionHash.Compute(entry.Key, partitions);
        var grain = grainFactory.GetGrain<IReplogShardGrain>($"{entry.TreeId}/{partition}");
        return await grain.AppendAsync(entry, cancellationToken).ConfigureAwait(false);
    }
}
