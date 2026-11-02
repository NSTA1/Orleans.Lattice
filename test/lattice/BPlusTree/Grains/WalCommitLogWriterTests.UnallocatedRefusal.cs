using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5708: the writer tells a leaf's fence when a whole append call was refused
/// without allocating anything, so the leaf releases the append it counted for its live
/// checkpoint advance. A refusal that may follow an allocation, or any ambiguous
/// failure, is never reported: releasing it could let the leaf advance past a record it
/// never applied.
/// </summary>
public partial class WalCommitLogWriterTests
{
    private static readonly Guid RefusalLeaf = new("5708aaaa-0000-0000-0000-000000000001");

    private sealed class CountingFence(LeafRetiredException? admissionRefusal = null) : ILeafAppendFence
    {
        private int _refused;

        public int RefusedUnallocated => Volatile.Read(ref _refused);

        public ValueTask<WalAppendStamp?> StampForAsync(IWalShardGrain shard, int partition, CancellationToken cancellationToken) =>
            ValueTask.FromResult<WalAppendStamp?>(WalAppendStamp.ForLeaf(RefusalLeaf, 1));

        public void OnStampedAppended(int partition, long unstampedWatermark)
        {
        }

        public void OnUnstampedAppended(int partition, long highestOffset)
        {
        }

        public void OnUnloaded(int partition, WalAppendStamp stamp)
        {
        }

        public LeafRetiredException OnStale(WalAppendStamp stamp) => LeafRetiredException.ForSupersededActivation(RefusalLeaf, 1);

        public LeafRetiredException? OnAdmitted(long admissionMark) => admissionRefusal;

        public void OnStampedMethodUnknown()
        {
        }

        public void OnClaimsExhausted()
        {
        }

        public void OnRefusedUnallocated() => Interlocked.Increment(ref _refused);
    }

    private static WalCommitLogWriter CreateFencedWriter(Func<int, IWalShardGrain> shardFor, int walPartitions = 1)
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>(), Arg.Any<string?>())
            .Returns(call => shardFor(int.Parse(((string)call[0]).Split('/')[1])));
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalPartitions = walPartitions, WalBatchedSingleEntryAppends = false });
        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);
        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns("site-test");
        return new WalCommitLogWriter(
            grainFactory,
            optionsMonitor,
            TestOptionsResolver.Create(baseOptions: optionsMonitor.Get(string.Empty), factory: grainFactory),
            modeResolver,
            clusterIdResolver);
    }

    private static WalRecord RefusalRecord(string key) => new()
    {
        TreeId = TreeId,
        Op = MutationKind.Set,
        Key = key,
        Value = [1],
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
    };

    /// <summary>Keys that hash to partitions 0 and 1 of two.</summary>
    private static (string P0, string P1) KeysOnTwoPartitions()
    {
        string? p0 = null, p1 = null;
        for (var i = 0; p0 is null || p1 is null; i++)
        {
            var key = $"k{i}";
            if (WalPartitionHash.Compute(key, 2) == 0)
            {
                p0 ??= key;
            }
            else
            {
                p1 ??= key;
            }
        }

        return (p0, p1);
    }

    private static WalStampBelowFloorException FloorRefusal() =>
        new(TreeId, 0, HybridLogicalClock.Zero, new HybridLogicalClock { WallClockTicks = 1 });

    [Test]
    public void A_self_fenced_point_append_is_reported_refused_without_allocation()
    {
        var shard = Substitute.For<IWalShardGrain>();
        var writer = CreateFencedWriter(_ => shard);
        var fence = new CountingFence(LeafRetiredException.ForSupersededActivation(RefusalLeaf, 1));

        Assert.ThrowsAsync<LeafRetiredException>(() => writer.AppendAsync(RefusalRecord("a"), fence, WalRoutingStamp.None, CancellationToken.None));
        Assert.That(fence.RefusedUnallocated, Is.EqualTo(1));
    }

    [Test]
    public void A_stale_generation_point_append_is_reported_refused_without_allocation()
    {
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendStampedAsync(Arg.Any<WalRecord>(), Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new WalStampedAppendResult(WalStampedBatchResult.Rejected, -1, WalAppendFenceOutcome.StaleGeneration)));
        shard.ClaimRoutingGenerationAsync(Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Refuse(2)));
        var writer = CreateFencedWriter(_ => shard);
        var fence = new CountingFence();

        Assert.ThrowsAsync<WalRoutingGenerationStaleException>(() =>
            writer.AppendAsync(RefusalRecord("a"), fence, WalRoutingStamp.ForShardRoot(0, 1), CancellationToken.None));
        Assert.That(fence.RefusedUnallocated, Is.EqualTo(1));
    }

    [Test]
    public void A_single_record_clock_floor_refusal_is_reported_refused_without_allocation()
    {
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendStampedAsync(Arg.Any<WalRecord>(), Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(FloorRefusal());
        var writer = CreateFencedWriter(_ => shard);
        var fence = new CountingFence();

        Assert.ThrowsAsync<WalStampBelowFloorException>(() => writer.AppendAsync(RefusalRecord("a"), fence, WalRoutingStamp.None, CancellationToken.None));
        Assert.That(fence.RefusedUnallocated, Is.EqualTo(1));
    }

    [Test]
    public void A_clock_floor_refusal_of_a_multi_record_slice_is_not_reported()
    {
        // The partition may have enqueued the slice's first records before it refused a
        // later one, so the call is not known to have allocated nothing.
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendBatchStampedAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(FloorRefusal());
        var writer = CreateFencedWriter(_ => shard);
        var fence = new CountingFence();

        Assert.ThrowsAsync<WalStampBelowFloorException>(() =>
            writer.AppendManyAsync([RefusalRecord("a"), RefusalRecord("b")], fence, WalRoutingStamp.None, CancellationToken.None));
        Assert.That(fence.RefusedUnallocated, Is.Zero);
    }

    [Test]
    public void A_batch_refused_at_every_partition_is_reported_once()
    {
        var (p0, p1) = KeysOnTwoPartitions();
        var shards = new[] { Substitute.For<IWalShardGrain>(), Substitute.For<IWalShardGrain>() };
        foreach (var shard in shards)
        {
            shard.AppendBatchStampedAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new WalStampedBatchResult([], 0, -1, WalAppendFenceOutcome.StaleGeneration)));
            shard.ClaimRoutingGenerationAsync(Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(WalAppendFenceClaimResult.Refuse(2)));
        }

        var writer = CreateFencedWriter(p => shards[p], walPartitions: 2);
        var fence = new CountingFence();

        Assert.ThrowsAsync<WalRoutingGenerationStaleException>(() =>
            writer.AppendManyAsync([RefusalRecord(p0), RefusalRecord(p1)], fence, WalRoutingStamp.ForShardRoot(0, 1), CancellationToken.None));
        Assert.That(fence.RefusedUnallocated, Is.EqualTo(1), "one call, one counted append to release");
    }

    [Test]
    public void A_batch_admitted_at_one_partition_and_refused_at_another_is_not_reported()
    {
        var (p0, p1) = KeysOnTwoPartitions();
        var admitted = Substitute.For<IWalShardGrain>();
        admitted.AppendBatchStampedAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new WalStampedBatchResult([7], 1, -1, WalAppendFenceOutcome.Admitted)));
        var refused = Substitute.For<IWalShardGrain>();
        refused.AppendBatchStampedAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new WalStampedBatchResult([], 0, -1, WalAppendFenceOutcome.StaleGeneration)));
        refused.ClaimRoutingGenerationAsync(Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Refuse(2)));
        var writer = CreateFencedWriter(p => p == 0 ? admitted : refused, walPartitions: 2);
        var fence = new CountingFence();

        Assert.ThrowsAsync<WalRoutingGenerationStaleException>(() =>
            writer.AppendManyAsync([RefusalRecord(p0), RefusalRecord(p1)], fence, WalRoutingStamp.ForShardRoot(0, 1), CancellationToken.None));
        Assert.That(fence.RefusedUnallocated, Is.Zero, "partition 0 allocated offset 7, which the leaf never applied");
    }

    [Test]
    public void An_ambiguous_append_failure_is_not_reported()
    {
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendStampedAsync(Arg.Any<WalRecord>(), Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new TimeoutException("the append may have landed"));
        var writer = CreateFencedWriter(_ => shard);
        var fence = new CountingFence();

        Assert.ThrowsAsync<TimeoutException>(() => writer.AppendAsync(RefusalRecord("a"), fence, WalRoutingStamp.None, CancellationToken.None));
        Assert.That(fence.RefusedUnallocated, Is.Zero);
    }
}
