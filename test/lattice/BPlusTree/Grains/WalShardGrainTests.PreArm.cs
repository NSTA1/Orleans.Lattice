using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the WAL shard's side of the pre-arm class and the arm (#5833, revision
/// 2): a pre-arm append needs no entry and writes no row but is fenced by its routing
/// generation; it never raises the shared unstamped watermark unless it carries no
/// generation; every leaf-authored record is attributed to its activation, so an arm's
/// foreign high covers every other activation's records, stamped or pre-arm; the
/// attribution resets at activation and folds conservatively; and the arm resolves a
/// deferred close's generation, never refused below one already loaded.
/// </summary>
public partial class WalShardGrainTests
{
    private static readonly Guid ActivationA = new("aaaaaaaa-0000-0000-0000-000000000001");
    private static readonly Guid ActivationB = new("bbbbbbbb-0000-0000-0000-000000000002");
    private static readonly WalRoutingInitiator PreArmRoot = WalRoutingInitiator.ShardRoot(0);

    private static WalAppendStamp PreArmStamp(Guid activation, long generation) =>
        WalAppendStamp.PreArm(FenceLeaf, activation) with { Initiator = PreArmRoot, Generation = generation };

    [Test]
    public async Task A_pre_arm_append_is_admitted_with_no_entry_and_writes_no_fence_row()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);

        var single = await grain.AppendStampedAsync(MakeEntry("a"), PreArmStamp(ActivationA, 1), CancellationToken.None);
        var batch = await grain.AppendBatchStampedAsync(new[] { MakeEntry("b"), MakeEntry("c") }, PreArmStamp(ActivationA, 1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(single.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
            Assert.That(batch.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
            Assert.That(batch.AdmittedCount, Is.EqualTo(2));
            Assert.That(storage.GrainIds(WalAppendFenceRows.EntryGrainType), Is.Empty, "a pre-arm append claimed an entry");
            Assert.That(storage.GrainIds(WalAppendFenceRows.DirectoryPageGrainType), Is.Empty, "a pre-arm append listed its leaf");
            Assert.That(storage.GrainIds(WalAppendFenceRows.GenerationGrainType), Is.Empty, "a pre-arm append claimed its generation");
        });
    }

    [Test]
    public async Task A_pre_arm_append_under_a_re_claimed_generation_or_at_the_floor_is_refused()
    {
        // PreArmSkipsGeneration and ForeignPreArmAfterReceiverArmed: the pre-arm class is
        // fenced by its routing generation exactly as a stamped append is.
        var storage = new EnumerableMemoryGrainStorage();
        await WalAppendFenceRows.RaiseRoutingFloorsAsync(storage, TreeId, new Dictionary<WalRoutingInitiator, long> { [PreArmRoot] = 2 });
        var grain = await CreateFencedGrainAsync(storage);
        var before = await grain.GetNextSequenceAsync(CancellationToken.None);

        var atFloor = await grain.AppendStampedAsync(MakeEntry("a"), PreArmStamp(ActivationA, 2), CancellationToken.None);
        var reclaim = await grain.ClaimRoutingGenerationAsync(PreArmRoot, 4, CancellationToken.None);
        var beforeReclaim = await grain.AppendStampedAsync(MakeEntry("b"), PreArmStamp(ActivationA, 3), CancellationToken.None);
        var after = await grain.GetNextSequenceAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(atFloor.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration), "a straddler at the floor was admitted pre-arm");
            Assert.That(reclaim.Accepted, Is.True);
            Assert.That(beforeReclaim.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration), "a write admitted before a re-claim was admitted pre-arm after it");
            Assert.That(after, Is.EqualTo(before), "a refused pre-arm append allocated an offset");
        });
    }

    [Test]
    public async Task A_routed_pre_arm_append_never_raises_the_shared_watermark_and_an_unrouted_one_does()
    {
        // PreArmSharedWatermark and UnroutedWriteAsPreArm (C4).
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var start = grain.UnstampedWatermarkForTesting;

        var routed = await grain.AppendStampedAsync(MakeEntry("a"), PreArmStamp(ActivationA, 1), CancellationToken.None);
        var afterRouted = grain.UnstampedWatermarkForTesting;
        var unrouted = await grain.AppendStampedAsync(MakeEntry("b"), WalAppendStamp.PreArm(FenceLeaf, ActivationA), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(routed.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
            Assert.That(afterRouted, Is.EqualTo(start), "a routed pre-arm record raised the shared watermark");
            Assert.That(unrouted.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted), "an unrouted pre-arm record was rejected rather than admitted unstamped");
            Assert.That(grain.UnstampedWatermarkForTesting, Is.EqualTo(unrouted.Offset), "an unrouted pre-arm record did not raise the shared watermark");
        });
    }

    [Test]
    public async Task The_foreign_high_counts_every_other_activation_s_records_stamped_or_pre_arm()
    {
        // ForeignHighIgnoreOtherToken and ForeignHighIgnoresEarlierStampedEpoch (B1).
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var start = grain.ForeignHighForTesting(FenceLeaf, ActivationB);

        var preArmA = await grain.AppendStampedAsync(MakeEntry("a"), PreArmStamp(ActivationA, 1), CancellationToken.None);
        var preArmB = await grain.AppendStampedAsync(MakeEntry("b"), PreArmStamp(ActivationB, 1), CancellationToken.None);
        var foreignToAAfterB = grain.ForeignHighForTesting(FenceLeaf, ActivationA);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        var stampedA = await grain.AppendStampedAsync(MakeEntry("c"), WalAppendStamp.ForLeaf(FenceLeaf, 1, ActivationA), CancellationToken.None);
        var third = Guid.NewGuid();

        Assert.Multiple(() =>
        {
            Assert.That(start, Is.LessThan(preArmA.Offset), "precondition: nothing attributed before the first append");
            Assert.That(foreignToAAfterB, Is.EqualTo(preArmB.Offset));
            Assert.That(grain.ForeignHighForTesting(FenceLeaf, ActivationB), Is.EqualTo(stampedA.Offset),
                "an earlier activation's stamped record is missing from the next activation's foreign high");
            Assert.That(grain.ForeignHighForTesting(FenceLeaf, ActivationA), Is.EqualTo(preArmB.Offset), "an activation's own records counted as foreign");
            Assert.That(grain.ForeignHighForTesting(FenceLeaf, third), Is.EqualTo(stampedA.Offset));
        });
    }

    [Test]
    public async Task A_fold_evicts_the_older_half_of_the_leaf_authors_and_floors_only_to_it()
    {
        // Review S2: one shard serves a whole (tree, partition), so a table past the bound
        // must not fold every leaf into the newest record's floor.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var leaves = Enumerable.Range(1, 4).Select(i => new Guid(i, 0, 0, new byte[8])).ToArray();
        var offsets = new long[leaves.Length];
        for (var i = 0; i < leaves.Length; i++)
        {
            var stamp = WalAppendStamp.PreArm(leaves[i], ActivationA) with { Initiator = PreArmRoot, Generation = 1 };
            offsets[i] = (await grain.AppendStampedAsync(MakeEntry($"k{i}"), stamp, CancellationToken.None)).Offset;
        }

        var restartFloor = grain.ForeignHighForTesting(Guid.NewGuid(), ActivationA);
        grain.FoldLeafAuthorsForTesting();

        // #5851 (O2b): an evicted leaf's records floor its own hash bucket only, so a leaf
        // reads the highest evicted offset among the leaves sharing its bucket.
        var buckets = (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)WalShardGrain.LeafAuthorFloorBucketCount);
        long BucketFloor(Guid leaf) => new[] { 0, 1 }
            .Where(e => WalShardGrain.LeafAuthorBucket(leaves[e], buckets) == WalShardGrain.LeafAuthorBucket(leaf, buckets))
            .Select(e => offsets[e])
            .DefaultIfEmpty(restartFloor)
            .Max();

        Assert.Multiple(() =>
        {
            Assert.That(grain.LeafAuthorCountForTesting, Is.EqualTo(2), "a fold dropped more than the older half");
            Assert.That(grain.LeafAuthorFoldsForTesting, Is.EqualTo(1));
            Assert.That(grain.ForeignHighForTesting(leaves[0], ActivationB), Is.EqualTo(BucketFloor(leaves[0])),
                "an evicted leaf's foreign high is the highest evicted offset of its bucket");
            Assert.That(grain.ForeignHighForTesting(leaves[0], ActivationB), Is.GreaterThanOrEqualTo(offsets[0]),
                "an eviction dropped a foreign record");
            Assert.That(grain.ForeignHighForTesting(leaves[3], ActivationB), Is.EqualTo(offsets[3]),
                "a kept leaf's foreign high was lost");
            Assert.That(grain.ForeignHighForTesting(leaves[2], ActivationA), Is.EqualTo(BucketFloor(leaves[2])),
                "the floor rose to the newest record, holding every arm back to it");
        });
    }

    [Test]
    public async Task An_eviction_raises_only_its_own_bucket_floor()
    {
        // #5851 (O2b): a leaf reads the floor of its own hash bucket, never another's, so
        // an eviction holds back the leaves sharing the evicted leaf's bucket and no other.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var buckets = (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)WalShardGrain.LeafAuthorFloorBucketCount);
        int Bucket(Guid leaf) => WalShardGrain.LeafAuthorBucket(leaf, buckets);
        Guid Find(Func<Guid, bool> wanted)
        {
            for (var i = 1; ; i++)
            {
                var candidate = new Guid(i, 5851, 0, new byte[8]);
                if (wanted(candidate))
                {
                    return candidate;
                }
            }
        }

        var evicted = Find(_ => true);
        var kept = Find(g => Bucket(g) != Bucket(evicted));
        var sharer = Find(g => g != evicted && Bucket(g) == Bucket(evicted));
        var outsider = Find(g => g != kept && Bucket(g) != Bucket(evicted) && Bucket(g) != Bucket(kept));
        var restartFloor = grain.ForeignHighForTesting(outsider, ActivationA);

        var evictedRecord = await grain.AppendStampedAsync(
            MakeEntry("evicted"), WalAppendStamp.PreArm(evicted, ActivationA) with { Initiator = PreArmRoot, Generation = 1 }, CancellationToken.None);
        var keptRecord = await grain.AppendStampedAsync(
            MakeEntry("kept"), WalAppendStamp.PreArm(kept, ActivationA) with { Initiator = PreArmRoot, Generation = 1 }, CancellationToken.None);
        grain.FoldLeafAuthorsForTesting();

        Assert.Multiple(() =>
        {
            Assert.That(grain.LeafAuthorCountForTesting, Is.EqualTo(1), "precondition: the fold evicted the lower entry only");
            Assert.That(restartFloor, Is.LessThan(evictedRecord.Offset), "precondition: the restart floor is below both records");
            Assert.That(grain.ForeignHighForTesting(evicted, ActivationB), Is.EqualTo(evictedRecord.Offset),
                "an eviction dropped the evicted leaf's foreign record");
            Assert.That(grain.ForeignHighForTesting(sharer, ActivationB), Is.EqualTo(evictedRecord.Offset),
                "a leaf sharing the evicted leaf's bucket must read its floor");
            Assert.That(grain.ForeignHighForTesting(outsider, ActivationB), Is.EqualTo(restartFloor),
                "an eviction raised a floor outside its own bucket");
            Assert.That(grain.ForeignHighForTesting(kept, ActivationA), Is.EqualTo(restartFloor),
                "a kept leaf in another bucket was held back to the evicted record");
            Assert.That(grain.ForeignHighForTesting(kept, ActivationB), Is.EqualTo(keptRecord.Offset));
        });
    }

    [Test]
    public async Task The_foreign_high_resets_below_the_recovered_head_and_never_drops_on_a_fold()
    {
        // PreArmWatermarkNotReset and EvictionDropsForeignHigh.
        var storage = new EnumerableMemoryGrainStorage();
        var provider = new InMemoryWalStorageProvider();
        var grain = await CreateFencedGrainAsync(storage, provider);
        var record = await grain.AppendStampedAsync(MakeEntry("a"), PreArmStamp(ActivationB, 1), CancellationToken.None);

        grain.FoldLeafAuthorsForTesting();
        var afterFold = grain.ForeignHighForTesting(FenceLeaf, ActivationA);
        var restarted = await CreateFencedGrainAsync(storage, provider);

        Assert.Multiple(() =>
        {
            Assert.That(afterFold, Is.GreaterThanOrEqualTo(record.Offset), "a fold dropped a foreign record");
            Assert.That(restarted.ForeignHighForTesting(FenceLeaf, ActivationA), Is.GreaterThanOrEqualTo(record.Offset),
                "a restart forgot a foreign record below the recovered head");
            Assert.That(restarted.LeafAuthorCountForTesting, Is.Zero);
        });
    }

    [Test]
    public async Task A_stamped_append_reports_a_foreign_pre_arm_record_of_its_own_leaf()
    {
        // DuplicatePreArmUnreported: a never-claimed duplicate's pre-arm record, allocated
        // after this activation armed, is reported on this activation's next own append.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var arm = await grain.ArmAppendEpochAsync(FenceLeaf, 1, ActivationA, WalRoutingInitiator.None, 0, CancellationToken.None);
        var duplicate = await grain.AppendStampedAsync(MakeEntry("dup"), PreArmStamp(ActivationB, 1), CancellationToken.None);
        var own = await grain.AppendStampedAsync(MakeEntry("own"), WalAppendStamp.ForLeaf(FenceLeaf, 1, ActivationA), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(arm.Accepted, Is.True);
            Assert.That(duplicate.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted), "a pre-arm append is admitted after another activation armed");
            Assert.That(own.UnstampedWatermark, Is.GreaterThanOrEqualTo(duplicate.Offset), "the duplicate's record went unreported to the armed activation");
        });
    }

    [Test]
    public async Task An_arm_returns_the_foreign_high_read_after_its_raise()
    {
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var foreign = await grain.AppendStampedAsync(MakeEntry("b"), PreArmStamp(ActivationB, 1), CancellationToken.None);
        var own = await grain.AppendStampedAsync(MakeEntry("a"), PreArmStamp(ActivationA, 1), CancellationToken.None);

        var arm = await grain.ArmAppendEpochAsync(FenceLeaf, 1, ActivationA, WalRoutingInitiator.None, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(arm.Accepted, Is.True);
            Assert.That(arm.ForeignHigh, Is.EqualTo(foreign.Offset), "the arm's foreign high missed another activation's record or counted its own");
            Assert.That(arm.Head, Is.GreaterThan(own.Offset));
            Assert.That(arm.ReclaimHead, Is.EqualTo(-1));
        });
    }

    [Test]
    public async Task An_arm_resolves_a_deferred_generation_so_a_write_admitted_before_it_is_refused()
    {
        // ReceiverArmBelowDeferredReclaimHead and ArmWithoutGenerationReclaim (C2): the
        // arm re-claims the deferred generation, after which the old one is refused, and
        // returns the re-claim's head.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var straddler = await grain.AppendStampedAsync(MakeEntry("old"), PreArmStamp(ActivationB, 1), CancellationToken.None);

        var arm = await grain.ArmAppendEpochAsync(FenceLeaf, 1, ActivationA, PreArmRoot, 2, CancellationToken.None);
        var late = await grain.AppendStampedAsync(MakeEntry("late"), PreArmStamp(ActivationB, 1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(arm.Accepted, Is.True);
            Assert.That(arm.ReclaimHead, Is.GreaterThan(straddler.Offset), "a record admitted before the re-claim lies at or above its head");
            Assert.That(late.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration), "the deferred generation was not re-claimed by the arm");
        });
    }

    [Test]
    public async Task An_arm_resolving_a_deferred_generation_below_a_loaded_one_takes_the_current_head()
    {
        // DeferredHeadRefusedBelowLoaded (C5): a later close re-claimed g'' > g' first.
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        await grain.ClaimRoutingGenerationAsync(PreArmRoot, 5, CancellationToken.None);
        var before = await grain.GetNextSequenceAsync(CancellationToken.None);

        var arm = await grain.ArmAppendEpochAsync(FenceLeaf, 1, ActivationA, PreArmRoot, 3, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(arm.Accepted, Is.True, "an arm resolving a deferred generation below the loaded one was refused");
            Assert.That(arm.ReclaimHead, Is.GreaterThanOrEqualTo(before));
            Assert.That(arm.ReclaimGeneration, Is.EqualTo(5),
                "review N2: the arm reported the generation it was asked for, not the one it claimed, so a close deferred between them stays held");
        });
    }

    [Test]
    public async Task An_arm_reports_the_deferred_generation_it_re_claimed()
    {
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());

        var deferred = await grain.ArmAppendEpochAsync(FenceLeaf, 1, ActivationA, PreArmRoot, 2, CancellationToken.None);
        var plain = await grain.ArmAppendEpochAsync(OtherFenceLeaf, 1, ActivationB, WalRoutingInitiator.None, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(deferred.ReclaimGeneration, Is.EqualTo(2));
            Assert.That(plain.ReclaimHead, Is.EqualTo(-1));
            Assert.That(plain.ReclaimGeneration, Is.Zero, "an arm with no deferred generation reported one");
        });
    }

    [Test]
    public async Task A_cold_load_racing_a_purge_retirement_installs_the_raised_floor()
    {
        // StraddlerBelowInitRoutingWalIgnoresFloor (R, 676f389a's trace): a cold load
        // reads the entry's row before a purge retires it, and lands after. The floor is
        // taken when the load installs, not when it read, so the retired generation is
        // never re-loaded and a straddler under it stays refused.
        var inner = new EnumerableMemoryGrainStorage();
        var seed = await CreateFencedGrainAsync(inner);
        await seed.ClaimRoutingGenerationAsync(PreArmRoot, 2, CancellationToken.None);
        var storage = new HeldFirstReadStorage(inner, WalAppendFenceRows.GenerationGrainType);
        var grain = await CreateFencedGrainAsync(storage);

        var load = grain.AppendStampedAsync(MakeEntry("s"), PreArmStamp(ActivationA, 2), CancellationToken.None);
        await storage.FirstReadDone.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await grain.RetireRoutingGenerationsAsync(shardCount: 1, CancellationToken.None);
        storage.Release.TrySetResult();
        var first = await load;
        var straddler = await grain.AppendStampedAsync(MakeEntry("t"), PreArmStamp(ActivationA, 2), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration), "the load re-installed a generation the purge retired");
            Assert.That(straddler.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration));
        });
    }

    [Test]
    public async Task A_second_arm_after_one_raise_writes_no_generation_row()
    {
        // PreArmHigherGenerationClaims (C1 cost witness): one generation row write per
        // (raise, partition), however many receivers arm after it.
        var storage = new SlowRowStorage(new EnumerableMemoryGrainStorage(), WalAppendFenceRows.GenerationGrainType);
        var grain = await CreateFencedGrainAsync(storage);
        await grain.ClaimRoutingGenerationAsync(PreArmRoot, 1, CancellationToken.None);
        var before = storage.Writes;

        var first = await grain.ArmAppendEpochAsync(FenceLeaf, 1, ActivationA, PreArmRoot, 2, CancellationToken.None);
        var afterFirst = storage.Writes;
        var second = await grain.ArmAppendEpochAsync(OtherFenceLeaf, 1, ActivationB, PreArmRoot, 2, CancellationToken.None);
        var pre = await grain.AppendStampedAsync(MakeEntry("p"), PreArmStamp(ActivationB, 3), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Accepted && second.Accepted, Is.True);
            Assert.That(afterFirst - before, Is.EqualTo(1), "the first arm after the raise did not re-claim it once");
            Assert.That(storage.Writes, Is.EqualTo(afterFirst), "a later arm re-wrote the generation row");
            Assert.That(second.ReclaimHead, Is.GreaterThanOrEqualTo(first.ReclaimHead));
            Assert.That(pre.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
            Assert.That(storage.Writes, Is.EqualTo(afterFirst), "a pre-arm append above the loaded generation claimed it");
        });
    }
}
