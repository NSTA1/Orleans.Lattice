using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The producer guarantee the live-advance core assumes of a key-moving close under the
/// pre-arm capability (#5833 revision 2, C2, C3 and C6; #5839): the shard root's raise of
/// its routing generation is durable before the receiver's marker records the deferred
/// head, and every write the shard root admits after the raise carries a generation at or
/// above the deferred one. No model checks the ordering of the two writes, so this
/// detector pins it. It is the code half of <c>HandOffBoundDeferredHeadBeforeRaise</c>.
/// </summary>
public sealed partial class ShardRootGrainSplitLinkTests
{
    private const string PreArmTreeId = "split-link-tree";

    [Test]
    public async Task A_deferred_close_is_written_only_after_the_raise_it_defers_is_durable()
    {
        var (h, storage, wal, donor, sibling) = await CreatePreArmSplitHarnessAsync();
        ReturnSplit(h, donor, sibling);

        await h.Grain.SetAsync("c", [1]);

        Assert.That(storage.DeferredCloseWrites, Is.Not.Empty, "precondition: the link closed the sibling by a deferred fence");
        var deferred = storage.DeferredCloseWrites.Max(w => w.Generation);
        Assert.Multiple(() =>
        {
            Assert.That(storage.DeferredCloseWrites.All(w => w.DurableGeneration >= w.Generation), Is.True,
                "every deferred head is written after the shard marker durably carries the generation it defers");
            Assert.That(h.Grain.AdmissionRoutingStamp().Generation, Is.GreaterThanOrEqualTo(deferred),
                "every write the shard root admits after the raise carries a generation at or above the deferred one");
        });

        // C6: the raise claims at no WAL partition; only the receiver's arm re-claims.
        await wal.DidNotReceive().ClaimRoutingGenerationAsync(Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_split_link_whose_raise_is_not_durable_writes_no_deferred_close()
    {
        var (h, storage, _, donor, sibling) = await CreatePreArmSplitHarnessAsync();
        ReturnSplit(h, donor, sibling);
        storage.FailShardMarkerWrites = true;

        try
        {
            await h.Grain.SetAsync("c", [1]);
        }
        catch (InvalidOperationException)
        {
            // The link may surface the injected fault; what matters is what was written.
        }

        Assert.That(storage.DeferredCloseWrites, Is.Empty,
            "a deferred head written before the raise is durable would leave a pending re-claim above every generation the shard root stamps after a crash");
    }

    [Test]
    public async Task A_deferred_reclaim_close_is_written_only_after_the_raise_it_defers_is_durable()
    {
        var (h, storage, wal, survivor, absorbed) = await CreatePreArmSplitHarnessAsync();
        await OpenLeafMarkerAsync(storage, survivor, ShardRootGrain.ReclaimHandOffToken(absorbed));

        await h.Grain.CloseReclaimHandOffForTestingAsync(survivor, absorbed);

        Assert.That(storage.DeferredCloseWrites, Is.Not.Empty, "precondition: the reclaim closed the survivor by a deferred fence");
        var deferred = storage.DeferredCloseWrites.Max(w => w.Generation);
        Assert.Multiple(() =>
        {
            Assert.That(storage.DeferredCloseWrites.All(w => w.DurableGeneration >= w.Generation), Is.True,
                "every deferred head is written after the shard marker durably carries the generation it defers");
            Assert.That(h.Grain.AdmissionRoutingStamp().Generation, Is.GreaterThanOrEqualTo(deferred));
        });
        await wal.DidNotReceive().ClaimRoutingGenerationAsync(Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_reclaim_whose_raise_is_not_durable_writes_no_deferred_close()
    {
        var (h, storage, _, survivor, absorbed) = await CreatePreArmSplitHarnessAsync();
        await OpenLeafMarkerAsync(storage, survivor, ShardRootGrain.ReclaimHandOffToken(absorbed));
        storage.FailShardMarkerWrites = true;

        await h.Grain.CloseReclaimHandOffForTestingAsync(survivor, absorbed);

        Assert.That(storage.DeferredCloseWrites, Is.Empty, "a deferred reclaim head was written before its raise was durable");
    }

    private static void ReturnSplit(Harness h, GrainId donor, GrainId sibling)
    {
        var split = Task.FromResult<SplitResult?>(new SplitResult
        {
            PromotedKey = "d",
            NewSiblingId = sibling,
            ChildIsLeaf = true,
            Donor = donor,
        });
        h.Leaf(donor).SetAsync("c", Arg.Any<byte[]>()).Returns(split);
        h.Leaf(donor).SetStampedAsync("c", Arg.Any<byte[]>(), Arg.Any<long>(), Arg.Any<WalRoutingStamp>()).Returns(split);
    }

    private static Task OpenLeafMarkerAsync(IGrainStorage storage, GrainId leaf, string token) =>
        HandOffMarkerRows.UpdateLeafAsync(storage, leaf.GetGuidKey(), m =>
        {
            m.Initialised = true;
            return HandOffMarkerCore.Open(m, token, null, null);
        });

    /// <summary>
    /// A one-level tree whose leaves are Guid-keyed (marker rows are keyed by the leaf's
    /// Guid), with the append-fence gate and the pre-arm class open, its hand-off rows in
    /// the recorder, and a WAL that accepts claims. A first write warms the shard's marker
    /// (seeding and fencing it claims at the WAL) before the WAL's calls are forgotten.
    /// The split's sibling marker is open, as the donor opens it at the sibling's birth.
    /// <paramref name="preArmView"/> is this silo's view of the pre-arm class (#5841, S1).
    /// <paramref name="gateOpenAfterWarmUp"/> is its view of the gate once the first write has warmed the marker.
    /// </summary>
    private static async Task<(Harness H, DeferredCloseOrderStorage Storage, IWalShardGrain Wal, GrainId Donor, GrainId Sibling)> CreatePreArmSplitHarnessAsync(bool preArmView = true, bool gateOpenAfterWarmUp = true)
    {
        var gate = Substitute.For<IWalAppendFenceGate>();
        gate.IsOpen.Returns(true);
        gate.IsPreArmOpen.Returns(preArmView);
        var services = new ServiceCollection().AddSingleton(gate).BuildServiceProvider();
        var h = CreateEmptyHarness(services);
        var donor = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        var right = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        var sibling = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        h.AddLeaf(donor);
        h.AddLeaf(right);
        h.AddLeaf(sibling);
        h.AddInternal(RootId, Table(true, (null, donor), ("m", right)));
        foreach (var leaf in new[] { donor, right, sibling })
        {
            h.Leaf(leaf).SetStampedAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<long>(), Arg.Any<WalRoutingStamp>())
                .Returns(Task.FromResult<SplitResult?>(null));
        }

        var storage = new DeferredCloseOrderStorage(new EnumerableMemoryGrainStorage(), PreArmTreeId);
        h.Grain.UseHandOffStorageForTesting(storage);
        var wal = Substitute.For<IWalShardGrain>();
        wal.ClaimRoutingGenerationAsync(Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Accept(10)));
        wal.GetNextSequenceAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<long>(10));
        h.Factory.GetGrain<IWalShardGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(wal);

        await h.Grain.SetAsync("a", [0]);
        wal.ClearReceivedCalls();
        gate.IsOpen.Returns(gateOpenAfterWarmUp);
        await OpenLeafMarkerAsync(storage, sibling, BPlusLeafGrain.SplitHandOffToken(donor.GetGuidKey()));
        storage.DeferredCloseWrites.Clear();
        return (h, storage, wal, donor, sibling);
    }

    /// <summary>
    /// Records, at each write of a leaf's hand-off marker that carries a deferred close,
    /// the routing generation the deferring shard root's marker row durably carries at
    /// that moment, read from the underlying store rather than from any cache.
    /// </summary>
    private sealed class DeferredCloseOrderStorage(EnumerableMemoryGrainStorage inner, string treeId) : IGrainStorage
    {
        public List<(int Shard, long Generation, long DurableGeneration)> DeferredCloseWrites { get; } = [];

        /// <summary>When set, every shard marker write throws, so no raise becomes durable.</summary>
        public bool FailShardMarkerWrites { get; set; }

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ReadStateAsync(stateName, grainId, grainState);

        public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (FailShardMarkerWrites && stateName == HandOffMarkerRows.ShardGrainType)
            {
                throw new InvalidOperationException("injected: the shard marker's raise is not durable");
            }

            if (stateName == HandOffMarkerRows.LeafGrainType && grainState.State is LeafHandOffState leaf)
            {
                foreach (var close in leaf.Closes.Where(c => c.DeferredGeneration > 0))
                {
                    var shard = await HandOffMarkerRows.ReadShardAsync(inner, treeId, close.DeferredShard);
                    DeferredCloseWrites.Add((close.DeferredShard, close.DeferredGeneration, shard.State.RoutingGeneration));
                }
            }

            await inner.WriteStateAsync(stateName, grainId, grainState);
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ClearStateAsync(stateName, grainId, grainState);
    }
}
