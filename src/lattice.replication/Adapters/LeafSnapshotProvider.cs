using System.Runtime.CompilerServices;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Adapters;

/// <summary>
/// Default <see cref="ILeafSnapshotProvider"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication(ISiloBuilder, System.Action{LatticeReplicationOptions}, bool)"/>.
/// Wraps the replication-package <see cref="ISnapshotProvider"/> with a
/// leaf-key range filter so a consumer can drain a single leaf's key range
/// without exporting the whole tree.
/// <para>
/// The current adapter consumes the streaming
/// <see cref="ISnapshotProvider.ExportAsync"/> output and applies a
/// client-side <c>[start, end)</c> filter; a future revision can route
/// the range hint into the producer side once a richer export shape is
/// available.
/// </para>
/// <para>
/// <b>Dormancy.</b> the dormant seam registers this adapter but no foreground
/// site invokes <see cref="StreamAsync"/>. the future replay coordinator's
/// <c>SnapshotThenWal</c> recovery path drives it when a leaf grain
/// has fallen off its WAL tail.
/// </para>
/// </summary>
internal sealed class LeafSnapshotProvider(
    ISnapshotProvider snapshotProvider,
    ICommitLogReader commitLogReader) : ILeafSnapshotProvider
{
    /// <inheritdoc />
    public async IAsyncEnumerable<LatticeMutation> StreamAsync(
        string treeId,
        int shardIndex,
        string leafKeyRangeStart,
        string? leafKeyRangeEnd,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        if (shardIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(shardIndex), shardIndex, "Shard index must be non-negative.");
        }
        ArgumentNullException.ThrowIfNull(leafKeyRangeStart);

        var stream = await snapshotProvider
            .ExportAsync(treeId, HybridLogicalClock.Zero, cancellationToken)
            .ConfigureAwait(false);

        await foreach (var entry in stream.Entries.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            // Half-open [start, end) range filter, ordinal comparison
            // - matches the lex order the rest of the library uses for
            // key ranges.
            if (string.CompareOrdinal(entry.Key, leafKeyRangeStart) < 0)
            {
                continue;
            }

            if (leafKeyRangeEnd is not null && string.CompareOrdinal(entry.Key, leafKeyRangeEnd) >= 0)
            {
                continue;
            }

            yield return new LatticeMutation
            {
                TreeId = treeId,
                Kind = MutationKind.Set,
                Key = entry.Key,
                EndExclusiveKey = null,
                Value = entry.Value,
                Timestamp = entry.Timestamp,
                IsTombstone = false,
                ExpiresAtTicks = 0,
                OriginClusterId = null,
                VectorClock = null,
                TransactionId = Guid.Empty,
                Category = MutationCategory.User,
                Delta = null,
            };
        }
    }

    /// <inheritdoc />
    public Task<long> GetSnapshotOffsetAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken = default)
        => commitLogReader.GetHeadOffsetAsync(treeId, shardIndex, cancellationToken);
}
