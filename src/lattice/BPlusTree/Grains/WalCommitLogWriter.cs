using System.Globalization;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ICommitLogWriter"/> registered by
/// <see cref="LatticeServiceCollectionExtensions.AddLattice(ISiloBuilder, System.Action{LatticeOptions}?)"/>.
/// Translates a core <see cref="LatticeMutation"/> to a wire-shaped
/// <see cref="WalRecord"/> via <see cref="WalRecordConverter"/> and
/// dispatches it to the per-shard
/// <see cref="IWalShardGrain.AppendAsync"/> entry point so the
/// caller observes the per-shard sequence number.
/// <para>
/// Bypasses <c>IReplogSink</c> by design — the replication-package sink
/// seam returns <see cref="System.Threading.Tasks.Task"/> rather than
/// <see cref="System.Threading.Tasks.Task{Long}"/>, and the leaf
/// commit path needs the assigned offset to drive replay coordination
/// after a leaf reactivation.
/// </para>
/// <para>
/// A complementary short-circuit on the replication mutation observer
/// suppresses double WAL appends from the post-commit observer dispatch
/// when the foreground commit path has already appended the same
/// mutation.
/// </para>
/// </summary>
internal sealed class WalCommitLogWriter(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> options,
    ILatticeMergeModeResolver modeResolver) : ICommitLogWriter
{
    /// <inheritdoc />
    public async Task<long> AppendAsync(LatticeMutation mutation, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var perTree = options.Get(mutation.TreeId);
        var partitions = perTree.WalPartitions;

        // Resolve the declared replication mode for this tree; default
        // to LwwRegister so a hand-constructed test call site still
        // gets a meaningful value on the wire. The foreground commit
        // path only invokes this adapter for trees the resolver
        // accepts, but adding a default here keeps the unit-test
        // surface trivially exercisable without a fully-configured
        // mode-resolver fake.
        var mode = modeResolver.Resolve(mutation.TreeId) ?? LatticeMergeMode.LwwRegister;

        // The origin cluster id is stamped upstream on the mutation by
        // the replication observer (when registered). Single-cluster
        // hosts have no cluster id, so the converter receives an empty
        // string and the resulting record's OriginClusterId is empty.
        var entry = WalRecordConverter.ToWalRecord(mutation, mode, originClusterId: "");

        // Saga terminal marks (TxCommit / TxAbort) have no natural
        // user key — the spec mandates one terminal append per WAL
        // partition the saga touched, never per-key. The shard-root
        // coordinator stamps its own shard index into mutation.Key as
        // a base-10 invariant-culture string; the writer maps that
        // shard index to a WAL partition by taking
        // <c>ShardIndex % partitions</c>. When the shard count exceeds
        // the WAL partition count, multiple shards collapse onto the
        // same WAL partition; receivers dedupe by transaction id, so
        // multiple terminal appends with the same TransactionId are
        // idempotent on the apply side.
        int partition;
        if (mutation.Kind is MutationKind.TxCommit or MutationKind.TxAbort)
        {
            if (!int.TryParse(entry.Key, NumberStyles.Integer, CultureInfo.InvariantCulture, out var shardIndex))
            {
                throw new InvalidOperationException(
                    $"Saga terminal mutation must carry the shard index in mutation.Key as a base-10 integer; got '{entry.Key}'.");
            }
            if (shardIndex < 0)
            {
                throw new InvalidOperationException(
                    $"Saga terminal mutation shard index {shardIndex} is negative for tree '{mutation.TreeId}'.");
            }
            partition = shardIndex % partitions;
        }
        else
        {
            partition = WalPartitionHash.Compute(entry.Key, partitions);
        }

        var grain = grainFactory.GetGrain<IWalShardGrain>($"{entry.TreeId}/{partition}");
        return await grain.AppendAsync(entry, cancellationToken).ConfigureAwait(false);
    }
}
