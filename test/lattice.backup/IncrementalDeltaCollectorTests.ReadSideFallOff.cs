using System.Runtime.CompilerServices;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Issue #4584: an incremental capture is not a WAL retention consumer. The GC may
/// trim past it, and the capture must then fall back to a full backup rather than
/// emit an increment with a silent gap - including when the trim lands after the
/// drain's pre-read tail probe, which only the read-side check can see.
/// </summary>
public sealed partial class IncrementalDeltaCollectorTests
{
    [Test]
    public async Task A_trim_landing_after_the_tail_probe_makes_the_capture_fall_back_rather_than_skip_entries()
    {
        var reader = new TrimOnReadCommitLogReader(entries: 5, trimBeforeOnRead: 3);
        var subscriber = new WalLogSubscriber(reader, new InMemoryWalCursorRegistry());
        var collector = new IncrementalDeltaCollector(
            _serializer,
            subscriber,
            treeId: "orders",
            consumerId: "test-consumer",
            partitions: 1,
            baseOffsets: new Dictionary<int, long> { [0] = 0 },
            startInclusive: null,
            endExclusive: null,
            mergeMode: BackupKeyMergeMode.LastWriterWins,
            baseBackupId: "base-id",
            batchSize: 100,
            resolveDecisions: (_, _) => Task.FromResult<IReadOnlyDictionary<Guid, TxStatus>>(new Dictionary<Guid, TxStatus>()));

        await foreach (var _ in collector.StreamAsync(CancellationToken.None))
        {
        }

        Assert.Multiple(() =>
        {
            Assert.That(collector.FellOffLog, Is.True,
                "offsets 1 and 2 were trimmed before the capture read them, so the increment must fall back");
            Assert.That(collector.KeyDescriptors, Is.Empty, "no entry past the trimmed range may be captured");
        });
    }

    /// <summary>
    /// A one-partition log whose tail probe reports nothing trimmed until a read
    /// starts, at which point every offset below <c>trimBeforeOnRead</c> is gone.
    /// </summary>
    private sealed class TrimOnReadCommitLogReader(int entries, long trimBeforeOnRead) : ICommitLogReader
    {
        private long _trimBefore;

        public async IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> ReadAsync(
            string treeId,
            int shardIndex,
            long fromOffsetExclusive,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            _trimBefore = trimBeforeOnRead;
            await Task.Yield();
            for (var offset = Math.Max(fromOffsetExclusive + 1, _trimBefore); offset < entries; offset++)
            {
                yield return (offset, new LatticeMutation
                {
                    TreeId = treeId,
                    Kind = MutationKind.Set,
                    Key = "k" + offset,
                    Value = [1],
                    OriginClusterId = "cluster-x",
                    Timestamp = new HybridLogicalClock { WallClockTicks = 100 + offset },
                });
            }
        }

        public Task<long> GetHeadOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default)
            => Task.FromResult((long)entries);

        public Task<long> GetTailOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken = default)
            => Task.FromResult(_trimBefore);
    }
}
