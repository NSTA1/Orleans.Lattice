using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for #5841 (a stale duplicate donor's epoch-only writes across a split) and
/// its fix, rule A: a Dekker pair on the leaf's epoch row and the leaf's own state. A claim
/// writes the row, then reads the leaf state fresh and goes stale on any ETag but its own
/// (<see cref="LeafStateCurrencyProbe"/>); a split persists its narrowing, then reads the row
/// and, when it exists, raises it and the leaf's entry at every partition
/// (<see cref="LeafSplitDuplicateFence"/>). Each test is named after the mutation of
/// <c>spec/wal/WalHandOffFence.tla</c> it detects. The WAL shard is real; the two
/// activations of the donor are real fences over one storage provider, which also holds
/// the leaf's state row.
/// </summary>
public partial class WalShardGrainTests
{
    private static readonly GrainId DonorStateId = GrainId.Create("leaf", FenceLeaf.ToString("N"));

    /// <summary>The fence gate as a silo views it.</summary>
    private sealed class DonorGate(bool preArm) : IWalAppendFenceGate
    {
        public bool IsOpen => true;

        public bool IsPreArmOpen => preArm;
    }

    /// <summary>One activation of the donor: its fence, the leaf-state ETag it holds, and its persist gate.</summary>
    private sealed class DonorActivation
    {
        public DonorActivation(IGrainStorage storage, string? etag, bool preArm, IGrainStorage? fenceStorage = null)
        {
            Etag = etag;
            Fence = new LeafAppendEpochFence(
                FenceLeaf,
                new DonorGate(preArm),
                fenceStorage ?? storage,
                Superseded.Add,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                LatticeTenantLabel.ForTree(TreeId),
                loadedStateCurrent: new LeafStateCurrencyProbe(storage, DonorStateId, () => Etag, PersistGate).IsCurrentAsync);
        }

        public string? Etag { get; set; }

        public SemaphoreSlim PersistGate { get; } = new(1, 1);

        public List<long> Superseded { get; } = new();

        public LeafAppendEpochFence Fence { get; }

        /// <summary>Persists the leaf's state as this activation holds it, conditionally on its ETag, as the leaf's persist does.</summary>
        public async Task PersistAsync(IGrainStorage storage, string? highKeyExclusive = null)
        {
            await PersistGate.WaitAsync();
            try
            {
                var row = new GrainState<LeafNodeState>(new LeafNodeState { TreeId = TreeId, HighKeyExclusive = highKeyExclusive })
                {
                    ETag = Etag,
                    RecordExists = Etag is not null,
                };
                await storage.WriteStateAsync(LeafStateCurrencyProbe.LeafStateName, DonorStateId, row);
                Etag = row.ETag;
            }
            finally
            {
                PersistGate.Release();
            }
        }
    }

    /// <summary>The live donor and a duplicate that both loaded the donor's state before the split.</summary>
    private static async Task<(EnumerableMemoryGrainStorage Storage, WalShardGrain Wal, DonorActivation Live, DonorActivation Duplicate)> TwoDonorActivationsAsync(
        bool preArm, IGrainStorage? duplicateFenceStorage = null)
    {
        var storage = new EnumerableMemoryGrainStorage();
        var wal = await CreateFencedGrainAsync(storage);
        var loaded = new GrainState<LeafNodeState>(new LeafNodeState { TreeId = TreeId });
        await storage.WriteStateAsync(LeafStateCurrencyProbe.LeafStateName, DonorStateId, loaded);
        var live = new DonorActivation(storage, loaded.ETag, preArm);
        var duplicate = new DonorActivation(storage, loaded.ETag, preArm, duplicateFenceStorage);
        return (storage, wal, live, duplicate);
    }

    /// <summary>The split: the live donor persists its narrowing, then fences, as the mode's closer runs it.</summary>
    private static async Task<long[]?> SplitAsync(EnumerableMemoryGrainStorage storage, WalShardGrain wal, DonorActivation live, bool preArm)
    {
        await live.PersistAsync(storage, highKeyExclusive: "m");
        return preArm
            ? await LeafSplitDuplicateFence.FenceAsync(storage, _ => wal, 1, FenceLeaf, null, null, CancellationToken.None)
            : await live.Fence.FenceSplitAsync(_ => wal, 1, CancellationToken.None);
    }

    /// <summary>Brings an activation to stamp under its epoch at the partition: an eager claim, or an arm in pre-arm mode.</summary>
    private static async Task ClaimOrArmAsync(DonorActivation activation, WalShardGrain wal, bool preArm)
    {
        if (preArm)
        {
            await activation.Fence.ArmAsync(_ => wal, [(0, WalRoutingInitiator.None, 0L)], CancellationToken.None);
        }
        else
        {
            await activation.Fence.StampForAsync(wal, 0, CancellationToken.None);
        }
    }

    /// <summary>An epoch-only write of a moved key (a <see cref="WalRoutingStamp.None"/> append), through the production dispatch.</summary>
    private static Task<long> EpochOnlyWriteAsync(WalShardGrain wal, DonorActivation activation) =>
        WalFencedAppendDispatch.AppendPointAsync(
            wal,
            MakeEntry("moved"),
            batched: false,
            partition: 0,
            activation.Fence,
            WalRoutingStamp.None with { AdmissionMark = activation.Fence.HandOffCloses + 1 },
            waitForDeadline: false,
            CancellationToken.None).AsTask();

    [TestCase(false)]
    [TestCase(true)]
    public async Task StaleDonorArmsAfterSplit5841(bool preArm)
    {
        // The defect as filed: a duplicate that loaded the donor before the split arms after
        // it - its claim writes the row above any bump - then appends an epoch-only write of
        // a moved key. Nothing routes, reports or replays it, so it is lost.
        var (storage, wal, live, duplicate) = await TwoDonorActivationsAsync(preArm);
        await SplitAsync(storage, wal, live, preArm);
        var before = await wal.GetNextSequenceAsync(CancellationToken.None);

        var arm = Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(() => ClaimOrArmAsync(duplicate, wal, preArm));
        var write = Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(() => EpochOnlyWriteAsync(wal, duplicate));

        Assert.Multiple(async () =>
        {
            Assert.That(arm!.ActivationSuperseded, Is.True, "the duplicate armed after the split");
            Assert.That(write!.ActivationSuperseded, Is.True, "the duplicate appended an epoch-only write after the split");
            Assert.That(await wal.GetNextSequenceAsync(CancellationToken.None), Is.EqualTo(before), "the duplicate's write was allocated");
            Assert.That(duplicate.Superseded, Has.Count.EqualTo(1));
            Assert.That(live.Superseded, Is.Empty);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task ClaimSkipsLeafStateCheck(bool preArm)
    {
        // An idle duplicate, which never claimed before the split, claims after it. The
        // split read no row, so nothing fenced it: only the claim's own state check can.
        var (storage, wal, live, duplicate) = await TwoDonorActivationsAsync(preArm);
        Assert.That(await SplitAsync(storage, wal, live, preArm), Is.Null, "precondition: no activation had claimed, so the split fenced nothing");

        var ex = Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(() => ClaimOrArmAsync(duplicate, wal, preArm));

        Assert.Multiple(async () =>
        {
            Assert.That(ex!.ActivationSuperseded, Is.True);
            Assert.That(duplicate.Fence.IsStale, Is.True, "a claimant whose leaf state moved on was not marked stale");
            Assert.That(await DurableEntryAsync(storage, FenceLeaf), Is.Zero, "the stale claimant claimed at a partition");
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task BumpOnlyIfSplittingActivationClaimed(bool preArm)
    {
        // The duplicate armed before the split; the splitting activation never did. The
        // split must raise the row and the entry because the row exists, whoever wrote it.
        var (storage, wal, live, duplicate) = await TwoDonorActivationsAsync(preArm);
        await ClaimOrArmAsync(duplicate, wal, preArm);
        Assert.That(live.Fence.HasClaimedAnywhere, Is.False, "precondition: the splitting activation is unarmed");

        var heads = await SplitAsync(storage, wal, live, preArm);
        var before = await wal.GetNextSequenceAsync(CancellationToken.None);
        var write = Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(() => EpochOnlyWriteAsync(wal, duplicate));

        Assert.Multiple(async () =>
        {
            Assert.That(heads, Is.Not.Null, "the split fenced nothing because its own activation had not claimed");
            Assert.That(await DurableEntryAsync(storage, FenceLeaf), Is.EqualTo(2), "the donor's entry was not raised above the duplicate's epoch");
            Assert.That(write!.ActivationSuperseded, Is.True);
            Assert.That(await wal.GetNextSequenceAsync(CancellationToken.None), Is.EqualTo(before), "the duplicate's epoch-only write was allocated");
        });
    }

    [Test]
    public async Task ClaimReadsStateBeforeRowWrite()
    {
        // The claim's half of the Dekker pair must write first and read second. Here the
        // duplicate's row write is in flight while the split persists and reads the row
        // (absent, so it fences nothing); only a state read issued after the row write
        // lands sees the narrowing.
        var gated = new GatedEpochRowStorage();
        var (storage, wal, live, duplicate) = await TwoDonorActivationsAsync(preArm: false, duplicateFenceStorage: gated);
        gated.Inner = storage;
        gated.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var claim = duplicate.Fence.StampForAsync(wal, 0, CancellationToken.None).AsTask();
        await WaitUntilAsync(() => gated.PendingWrites == 1);
        Assert.That(await SplitAsync(storage, wal, live, preArm: false), Is.Null, "precondition: the split read no row");
        gated.Gate.SetResult();

        var ex = Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(() => claim);
        Assert.Multiple(async () =>
        {
            Assert.That(ex!.ActivationSuperseded, Is.True, "the claim read the leaf state before its row write landed");
            Assert.That(await DurableEntryAsync(storage, FenceLeaf), Is.Zero);
        });
    }

    [Test]
    public async Task StaleClaimantAppends()
    {
        // A4: a claimant found stale claims nowhere, and every append waiting on its claim
        // sees the supersession; none goes unstamped instead.
        var (storage, wal, live, duplicate) = await TwoDonorActivationsAsync(preArm: false);
        await SplitAsync(storage, wal, live, preArm: false);
        var before = await wal.GetNextSequenceAsync(CancellationToken.None);

        var first = EpochOnlyWriteAsync(wal, duplicate);
        var second = EpochOnlyWriteAsync(wal, duplicate);
        Exception? firstFault = null, secondFault = null;
        try { await first; } catch (Exception ex) { firstFault = ex; }
        try { await second; } catch (Exception ex) { secondFault = ex; }

        Assert.Multiple(async () =>
        {
            Assert.That(firstFault, Is.TypeOf<Orleans.Lattice.LeafRetiredException>());
            Assert.That(secondFault, Is.TypeOf<Orleans.Lattice.LeafRetiredException>(), "a waiter on the stale claim appended");
            Assert.That(await wal.GetNextSequenceAsync(CancellationToken.None), Is.EqualTo(before), "a stale claimant's append was allocated");
            Assert.That(await DurableEntryAsync(storage, FenceLeaf), Is.Zero);
        });
    }

    [Test]
    public async Task EpochOnlyWriteUnfenced()
    {
        // The WAL shard's side: once the split raised the donor's entry, an epoch-only
        // append at the duplicate's older epoch is refused and allocates nothing.
        var (storage, wal, live, duplicate) = await TwoDonorActivationsAsync(preArm: false);
        var stamp = await duplicate.Fence.StampForAsync(wal, 0, CancellationToken.None);
        await SplitAsync(storage, wal, live, preArm: false);
        var before = await wal.GetNextSequenceAsync(CancellationToken.None);

        var refused = await wal.AppendStampedAsync(MakeEntry("moved"), stamp!.Value, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(refused.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleEpoch));
            Assert.That(refused.Offset, Is.EqualTo(WalStampedBatchResult.Rejected));
            Assert.That(await wal.GetNextSequenceAsync(CancellationToken.None), Is.EqualTo(before));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task SplitRowWriteDropsBump(bool preArm)
    {
        // When the row exists the split writes it plus one and raises the leaf's own entry
        // to that value at every partition - never the partition's shared floor, which every
        // other leaf's claims are checked against.
        var (storage, wal, live, duplicate) = await TwoDonorActivationsAsync(preArm);
        await ClaimOrArmAsync(duplicate, wal, preArm);
        await wal.ClaimAppendEpochAsync(OtherFenceLeaf, 1, CancellationToken.None);

        await SplitAsync(storage, wal, live, preArm);
        var other = await wal.ClaimAppendEpochAsync(OtherFenceLeaf, 1, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That((await WalAppendFenceRows.ReadLeafEpochAsync(storage, FenceLeaf)).State.Epoch, Is.EqualTo(2), "the split did not bump the row");
            Assert.That(await DurableEntryAsync(storage, FenceLeaf), Is.EqualTo(2), "the split did not raise the leaf's entry");
            Assert.That(wal.EpochFloorForTesting, Is.Zero, "the split raised the partition's shared floor");
            Assert.That(other.Accepted, Is.True, "another leaf's claim was refused after the split");
        });
    }

    [Test]
    public async Task SplitBumpUsesCachedEtag()
    {
        // A3: the live donor claimed, then a duplicate claimed above it before the split.
        // The split writes on the ETag it just read, not the live activation's cached one,
        // and the live activation adopts the row it wrote, so it neither supersedes itself
        // now nor at its next row write.
        var (storage, wal, live, duplicate) = await TwoDonorActivationsAsync(preArm: false);
        await live.Fence.StampForAsync(wal, 0, CancellationToken.None);
        await duplicate.Fence.StampForAsync(wal, 0, CancellationToken.None);

        await SplitAsync(storage, wal, live, preArm: false);
        var next = live.Fence.StampForAsync(wal, 0, CancellationToken.None);
        var nextSynchronous = next.IsCompletedSuccessfully;
        var stamp = await next;

        // The live activation's next row write: a refusal at a partition it has not claimed
        // (its entry already above the raised epoch) bumps the row on the activation's
        // ETag, which must be the one the split wrote, not the one it cached before.
        await wal.ClaimAppendEpochAsync(FenceLeaf, 5, CancellationToken.None);
        var bumped = await live.Fence.StampForAsync(wal, 1, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(live.Superseded, Is.Empty, "the live activation's row write conflicted on a cached ETag and superseded it");
            Assert.That(stamp?.Epoch, Is.EqualTo(3), "the live activation did not adopt the raised epoch");
            Assert.That(nextSynchronous, Is.True, "the live activation did not adopt the split's claim at the partition");
            Assert.That(bumped?.Epoch, Is.EqualTo(6), "the bump after the split did not raise above the refusal");
        });
    }

    [Test]
    public async Task A_claim_racing_its_own_in_flight_persist_stays_live()
    {
        // A1: the claim's state read is serialised with the activation's own persists, so a
        // persist in flight is compared with the ETag it lands, never read as another's.
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: false);
        await live.PersistGate.WaitAsync();
        var landed = new GrainState<LeafNodeState>(new LeafNodeState { TreeId = TreeId }) { ETag = live.Etag, RecordExists = true };
        await storage.WriteStateAsync(LeafStateCurrencyProbe.LeafStateName, DonorStateId, landed);

        var claim = live.Fence.StampForAsync(wal, 0, CancellationToken.None).AsTask();
        await Task.Delay(50);
        live.Etag = landed.ETag;
        live.PersistGate.Release();
        var stamp = await claim;

        Assert.Multiple(() =>
        {
            Assert.That(stamp?.Epoch, Is.EqualTo(1));
            Assert.That(live.Superseded, Is.Empty, "the activation's own in-flight persist read as another activation's");
        });
    }

    [Test]
    public void A_missing_leaf_row_and_an_empty_etag_name_the_same_version()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LeafStateCurrencyProbe.EtagsEqual(null, string.Empty), Is.True);
            Assert.That(LeafStateCurrencyProbe.EtagsEqual(string.Empty, null), Is.True);
            Assert.That(LeafStateCurrencyProbe.EtagsEqual("1", "1"), Is.True);
            Assert.That(LeafStateCurrencyProbe.EtagsEqual("1", "2"), Is.False);
            Assert.That(LeafStateCurrencyProbe.EtagsEqual(null, "1"), Is.False);
        });
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(5);
        }

        Assert.That(condition(), Is.True, "the condition was not reached in time");
    }

    /// <summary>Holds every write of a leaf epoch row behind <see cref="Gate"/> while it is set.</summary>
    private sealed class GatedEpochRowStorage : IGrainStorage
    {
        private int _pending;

        public IGrainStorage Inner { get; set; } = null!;

        public TaskCompletionSource? Gate { get; set; }

        public int PendingWrites => Volatile.Read(ref _pending);

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            Inner.ReadStateAsync(stateName, grainId, grainState);

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            Inner.ClearStateAsync(stateName, grainId, grainState);

        public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (Gate is { } gate && grainState is IGrainState<LeafAppendEpochState>)
            {
                Interlocked.Increment(ref _pending);
                try
                {
                    await gate.Task;
                }
                finally
                {
                    Interlocked.Decrement(ref _pending);
                }
            }

            await Inner.WriteStateAsync(stateName, grainId, grainState);
        }
    }
}
