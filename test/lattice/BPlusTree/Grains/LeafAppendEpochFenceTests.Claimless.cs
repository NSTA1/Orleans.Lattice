using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the fence's side of the claimless advance and the donor's own split close
/// (#5837): a pre-arm partition has a claimless view only on a report that covered every
/// donor close pending now, and none while more are pending than a stamp carries; a drain
/// waits exactly for the appends that started before it; and the armed split window
/// records each own moved-key offset, marks a failed moved-key append ambiguous, and keeps
/// the routing generation the activation's writes were admitted under.
/// </summary>
public sealed partial class LeafAppendEpochFenceTests
{
    private sealed class PreArmGate : IWalAppendFenceGate
    {
        public bool IsOpen => true;

        public bool IsPreArmOpen => true;
    }

    private static readonly WalPendingClose CloseA = new(new Guid("da000000-0000-0000-0000-0000000000da"), new Guid("ca000000-0000-0000-0000-0000000000ca"), 0, 3);
    private static readonly WalPendingClose CloseB = new(new Guid("db000000-0000-0000-0000-0000000000db"), new Guid("cb000000-0000-0000-0000-0000000000cb"), 0, 3);
    private static readonly WalPendingClose CloseC = new(new Guid("dc000000-0000-0000-0000-0000000000dc"), new Guid("cc000000-0000-0000-0000-0000000000cc"), 0, 3);

    private static LeafAppendEpochFence PreArmFence() => new(
        Leaf,
        new PreArmGate(),
        new EnumerableMemoryGrainStorage(),
        _ => { },
        new KeyValuePair<string, object?>(LatticeMetrics.TagTree, "tree"),
        LatticeTenantLabel.ForTree("tree"));

    private static WalRecord Record(string key) => new()
    {
        TreeId = "tree",
        Op = MutationKind.Set,
        Key = key,
        Value = [1],
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
    };

    private static async Task<WalAppendStamp> PreArmStampAsync(LeafAppendEpochFence fence) =>
        (await fence.StampForAsync(Substitute.For<IWalShardGrain>(), partition: 0, CancellationToken.None))!.Value;

    [Test]
    public async Task A_pre_arm_partition_has_a_claimless_view_only_once_a_report_arrived()
    {
        var fence = PreArmFence();
        var stamp = await PreArmStampAsync(fence);
        var before = fence.TryGetPreArmView(0, out _);

        fence.OnStampedAppended(0, 12, stamp);
        var after = fence.TryGetPreArmView(0, out var bound);

        Assert.Multiple(() =>
        {
            Assert.That(stamp.IsPreArm, Is.True, "precondition: the gate is in pre-arm");
            Assert.That(before, Is.False, "a partition with no report had a claimless view");
            Assert.That(after, Is.True);
            Assert.That(bound, Is.EqualTo(12));
        });
    }

    [Test]
    public async Task A_claimless_view_needs_a_report_that_carried_every_pending_close()
    {
        // FreeAdvancePassesPendingDeferredHead: a report from before the close was learned
        // does not carry the donor's stale high.
        var fence = PreArmFence();
        var withoutClose = await PreArmStampAsync(fence);
        fence.OnStampedAppended(0, 4, withoutClose);

        fence.SetPendingCloses([CloseA]);
        var uncovered = fence.TryGetPreArmView(0, out _);
        var withClose = await PreArmStampAsync(fence);
        fence.OnStampedAppended(0, 20, withClose);
        var covered = fence.TryGetPreArmView(0, out var bound);
        fence.OnStampedAppended(0, 3, withoutClose);
        var narrowedByALateReport = fence.TryGetPreArmView(0, out _);

        Assert.Multiple(() =>
        {
            Assert.That(withClose.Close1, Is.EqualTo(CloseA), "a stamp did not carry the pending close");
            Assert.That(uncovered, Is.False, "a report that did not carry the pending close gave a claimless view");
            Assert.That(covered, Is.True);
            Assert.That(bound, Is.EqualTo(20));
            Assert.That(narrowedByALateReport, Is.False, "a late report that did not carry the close kept the view");
        });
    }

    [Test]
    public async Task More_pending_closes_than_a_stamp_carries_hold_the_claimless_advance()
    {
        var fence = PreArmFence();
        fence.SetPendingCloses([CloseA, CloseB, CloseC]);
        var stamp = await PreArmStampAsync(fence);
        fence.OnStampedAppended(0, 9, stamp);

        Assert.Multiple(() =>
        {
            Assert.That((stamp.Close1, stamp.Close2), Is.EqualTo((CloseA, CloseB)));
            Assert.That(fence.TryGetPreArmView(0, out _), Is.False, "an over-cap close was dropped rather than holding the advance");
        });

        fence.SetPendingCloses([CloseA, CloseB]);
        Assert.That(fence.TryGetPreArmView(0, out _), Is.True, "the hold outlived the excess close");
    }

    [Test]
    public async Task A_drain_waits_for_the_appends_that_started_before_it_only()
    {
        // DonorCloseBeforeDrain.
        var fence = PreArmFence();
        var before = fence.BeginAppend(WalRoutingStamp.None);

        var drain = fence.DrainAsync();
        var after = fence.BeginAppend(WalRoutingStamp.None);
        fence.EndAppend(after);
        var waitedOnlyForEarlier = !drain.IsCompleted;
        fence.EndAppend(before);
        await drain.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(waitedOnlyForEarlier, Is.True, "the drain completed before an append that started before it ended");
            Assert.That(drain.IsCompletedSuccessfully, Is.True);
            Assert.That(fence.DrainAsync().IsCompleted, Is.True, "a drain with nothing in flight waited");
        });
    }

    [Test]
    public void The_split_window_records_each_own_moved_key_offset()
    {
        // DonorCloseHeadAtHeads: the close head is one above the highest own moved-key record.
        var fence = PreArmFence();
        fence.ArmSplitWindow("m", "x", partitions: 2);

        fence.NoteOwnAppend(0, Record("n"), 5);
        fence.NoteOwnAppend(0, Record("o"), 3);
        fence.NoteOwnAppend(1, Record("a"), 9);
        fence.NoteOwnAppend(1, Record("y"), 11);
        fence.TryGetSplitWindow(out var high, out _, out _, out var ambiguous);

        Assert.Multiple(() =>
        {
            Assert.That(high, Is.EqualTo(new long[] { 5, -1 }), "a kept or out-of-range key was counted, or a moved key was not");
            Assert.That(ambiguous, Is.False);
        });
    }

    [Test]
    public void A_failed_moved_key_append_makes_the_window_ambiguous()
    {
        var fence = PreArmFence();
        fence.ArmSplitWindow("m", null, partitions: 1);

        fence.NoteFailedAppend(Record("a"));
        fence.TryGetSplitWindow(out _, out _, out _, out var afterKept);
        fence.NoteFailedAppend(Record("q"));
        fence.TryGetSplitWindow(out _, out _, out _, out var afterMoved);
        fence.DisarmSplitWindow();

        Assert.Multiple(() =>
        {
            Assert.That(afterKept, Is.False, "a failed append of a kept key made the window ambiguous");
            Assert.That(afterMoved, Is.True, "a failed moved-key append, which may have landed, was not noted");
            Assert.That(fence.TryGetSplitWindow(out _, out _, out _, out _), Is.False, "the window outlived its disarm");
        });
    }

    [Test]
    public void The_window_keeps_the_routing_generation_the_writes_were_admitted_under()
    {
        var fence = PreArmFence();
        fence.ArmSplitWindow("m", null, partitions: 1);

        fence.EndAppend(fence.BeginAppend(WalRoutingStamp.ForShardRoot(3, 7)));
        fence.EndAppend(fence.BeginAppend(WalRoutingStamp.ForShardRoot(3, 5)));
        fence.TryGetSplitWindow(out _, out var shard, out var generation, out _);
        fence.EndAppend(fence.BeginAppend(WalRoutingStamp.ForShardRoot(4, 2)));
        fence.TryGetSplitWindow(out _, out var movedShard, out var movedGeneration, out _);

        Assert.Multiple(() =>
        {
            Assert.That((shard, generation), Is.EqualTo((3, 7L)), "a lower generation lowered the admitted generation");
            Assert.That((movedShard, movedGeneration), Is.EqualTo((4, 2L)), "a new shard root's generation was compared with the old one's");
        });
    }

    [Test]
    public async Task A_forwarded_write_s_stamp_carries_the_pending_closes()
    {
        // MF1: the stamp of a forward the receiver appends carries its pending closes, so
        // the response reports the donor's stale high like every other routed append.
        var fence = PreArmFence();
        fence.SetPendingCloses([CloseA]);
        try
        {
            Orleans.Runtime.RequestContext.Set(WalForwardChain.ContextKey, WalForwardChain.Extend(null, CloseA.Donor, Leaf));

            var stamp = await PreArmStampAsync(fence);

            Assert.Multiple(() =>
            {
                Assert.That(stamp.Forwarder, Is.EqualTo(CloseA.Donor), "precondition: the write was forwarded");
                Assert.That(stamp.Close1, Is.EqualTo(CloseA), "a forwarded write's stamp dropped the pending close");
            });
        }
        finally
        {
            Orleans.Runtime.RequestContext.Clear();
        }
    }

    [Test]
    public async Task A_relinquished_donor_keeps_the_epoch_its_own_split_fence_adopts()
    {
        // RS2 (a5e2e2ef on #5837): under rule S the counted close relinquishes the donor's
        // claim (#5840 S4) just before its own fence raises the row. R2 discards a claim in
        // flight, never the fence's adopting install, so the donor stamps at the raised
        // epoch at once, with no fresh claim.
        var h = new Harness();
        await h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        var before = await h.RowEpochAsync();

        h.Fence.CountSplitClose();
        await h.Fence.FenceSplitAsync(_ => h.Shard, partitionCount: 1, CancellationToken.None);
        var raised = await h.RowEpochAsync();
        var next = h.Fence.StampForAsync(h.Shard, partition: 0, CancellationToken.None);
        var synchronous = next.IsCompletedSuccessfully;
        var stamp = await next;

        Assert.Multiple(async () =>
        {
            Assert.That(raised, Is.GreaterThan(before), "precondition: the fence raised the row");
            Assert.That(synchronous, Is.True, "the fence's install was discarded, so the donor claimed afresh");
            Assert.That(stamp!.Value.Epoch, Is.EqualTo(raised), "the donor does not stamp at the epoch its fence adopted");
            Assert.That(await h.RowEpochAsync(), Is.EqualTo(raised), "the donor raised its row again after its own fence");
        });
    }
}
