using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalShardGrainTests
{
    // Issue #4621: a flush abandoned at its WalFlushTimeout deadline is removed
    // from the in-flight chain, and the failure handler resyncs the allocator past
    // a later window that already landed. The abandoned provider call can still
    // land afterwards. Without a bound on the watermark, a reader is shown the
    // later entry, advances its cursor past the abandoned offset, and never sees
    // that offset when it lands - while a reader from below does.

    [Test]
    public async Task ReadAsync_never_exposes_an_offset_above_an_abandoned_flush_that_can_still_land()
    {
        var gate = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var grain = await CreateGrainAsync(gate, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalFlushTimeout = TimeSpan.FromMilliseconds(300),
        });

        var abandoned = grain.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await gate.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var acknowledged = await grain.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        Assert.That(acknowledged, Is.EqualTo(1L), "the later window lands");
        Assert.That(async () => await abandoned, Throws.TypeOf<TimeoutException>(), "the gated window is abandoned");

        // A cursor reader polls while the abandoned call is still outstanding.
        var page = await grain.ReadAsync(0L, 256, CancellationToken.None);
        var cursor = page.NextSequence;

        // The abandoned call lands late. Its read bound lifts only once the call has
        // settled, which is after it signals the landing (#5730).
        gate.Open();
        await gate.Landed.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await WaitForAbandonedWindowsToSettleAsync(gate);

        var seenByCursor = page.Entries.Select(e => e.Sequence).ToList();
        var next = await grain.ReadAsync(cursor, 256, CancellationToken.None);
        seenByCursor.AddRange(next.Entries.Select(e => e.Sequence));
        var fromBelow = (await grain.ReadAsync(0L, 256, CancellationToken.None)).Entries.Select(e => e.Sequence).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(fromBelow, Is.EqualTo(new[] { 0L, 1L }), "the late landing is readable from below");
            Assert.That(seenByCursor, Is.EqualTo(fromBelow),
                "a cursor reader must see exactly what a reader from below sees, in order");
        });

        // Once the abandoned call has settled, the shard serves as usual.
        var third = await grain.AppendAsync(MakeEntry("k2"), CancellationToken.None);
        Assert.That(third, Is.EqualTo(2L));
        await WaitForOwnFlushToSettleAsync(grain, third);
        Assert.That((await grain.ReadAsync(0L, 256, CancellationToken.None)).Entries.Select(e => e.Sequence),
            Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task ReadAsync_releases_a_hole_once_the_abandoned_flush_settles_without_landing()
    {
        var gate = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var grain = await CreateGrainAsync(gate, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalFlushTimeout = TimeSpan.FromMilliseconds(300),
        });

        var abandoned = grain.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await gate.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await grain.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        Assert.That(async () => await abandoned, Throws.TypeOf<TimeoutException>());

        Assert.That((await grain.ReadAsync(0L, 256, CancellationToken.None)).Entries, Is.Empty,
            "nothing above the abandoned offset is exposed while its call is outstanding");

        // The abandoned call settles without writing: the hole is permanent, so
        // the entry above it is released.
        gate.Fail();
        await gate.Settled.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var deadline = Environment.TickCount64 + 15_000;
        WalShardPage page;
        do
        {
            page = await grain.ReadAsync(0L, 256, CancellationToken.None);
            if (page.Entries.Count > 0)
            {
                break;
            }

            await Task.Delay(10);
        }
        while (Environment.TickCount64 < deadline);

        Assert.That(page.Entries.Select(e => e.Sequence), Is.EqualTo(new[] { 1L }),
            "a settled hole no longer holds back the entries above it");
    }

    [Test]
    public async Task A_reactivated_shard_is_held_below_its_predecessors_abandoned_flush_until_it_settles()
    {
        var provider = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var options = new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalFlushTimeout = TimeSpan.FromMilliseconds(300),
        };
        var first = await CreateGrainAsync(provider, options);
        var abandoned = first.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await provider.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await first.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        Assert.That(async () => await abandoned, Throws.TypeOf<TimeoutException>());

        // A new activation of the same shard knows nothing of its predecessor's
        // outstanding call; it recovers its allocator past offset 1.
        var second = await CreateGrainAsync(provider, options);
        var page = await second.ReadAsync(0L, 256, CancellationToken.None);
        Assert.That(page.Entries, Is.Empty, "the new activation exposes nothing above the outstanding window");

        provider.Open();
        await provider.Landed.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await WaitForAbandonedWindowsToSettleAsync(provider);
        Assert.That((await second.ReadAsync(page.NextSequence, 256, CancellationToken.None)).Entries.Select(e => e.Sequence),
            Is.EqualTo(new[] { 0L, 1L }), "the cursor reader sees the late landing in order");
    }

    [Test]
    public async Task A_head_a_reader_resumes_from_never_passes_an_offset_a_recovered_allocator_can_reissue()
    {
        // 57704fda's two-fault trace: a reader captures the head while the last
        // flush is still in motion, that flush dies without landing (the shard is
        // lost), and the recovered allocator - highest stored + 1 - issues the same
        // offset again. The new write must not land below the reader's position.
        var provider = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var options = new LatticeOptions { WalMaxBatchEntries = 1, WalMaxPendingBatches = 8 };
        var first = await CreateGrainAsync(provider, options);
        var inFlight = first.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await provider.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(first);
        var reader = new WalCommitLogReader(factory);
        var head = await reader.GetHeadOffsetAsync(TreeId, 0);

        // The flush dies without landing and the shard comes back cold.
        provider.Fail();
        await provider.Settled.Task.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.That(async () => await inFlight, Throws.Exception);
        var second = await CreateGrainAsync(provider, options);
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(second);
        var reissued = await second.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        await WaitForOwnFlushToSettleAsync(second, reissued);

        var seen = (await second.ReadAsync(head, 256, CancellationToken.None)).Entries.Select(e => e.Sequence).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(reissued, Is.EqualTo(0L), "the recovered allocator issues offset 0 again");
            Assert.That(head, Is.LessThanOrEqualTo(reissued), "the head a reader resumes from must not pass the reissued offset");
            Assert.That(seen, Does.Contain(reissued), "a reader resuming from the head sees the new write");
        });
    }

    [Test]
    public async Task A_trailing_hole_left_by_a_force_faulted_flush_is_not_exposed_and_its_reissue_is_seen()
    {
        // The deactivation drain cancels an in-flight flush, whose provider call is
        // still in motion. When that call then settles without landing, the window
        // is a trailing hole: nothing is stored above it, and a recovered allocator
        // issues it again. The readable head must not pass it. (Here the failure
        // handler's resync rewinds the allocator before the call settles; the head
        // clamp's own detector, where that resync fails, is
        // A_trailing_hole_is_not_exposed_when_the_post_failure_resync_fails.)
        var provider = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var options = new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalDrainBudget = TimeSpan.FromMilliseconds(200),
        };
        var first = await CreateGrainAsync(provider, options);
        var inFlight = first.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await provider.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await first.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None);
        Assert.That(async () => await inFlight, Throws.Exception);

        provider.Fail();
        await provider.Settled.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var head = await first.GetReadableHeadAsync(CancellationToken.None);

        var second = await CreateGrainAsync(provider, options);
        var reissued = await second.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        await WaitForOwnFlushToSettleAsync(second, reissued);
        var seen = (await second.ReadAsync(head, 256, CancellationToken.None)).Entries.Select(e => e.Sequence).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(head, Is.EqualTo(0L), "a trailing hole is not exposed: nothing is stored above it");
            Assert.That(reissued, Is.EqualTo(0L));
            Assert.That(seen, Does.Contain(reissued), "a reader resuming from the head sees the reissued write");
        });
    }

    [Test]
    public async Task A_trailing_hole_is_not_exposed_when_the_post_failure_resync_fails()
    {
        // The head clamp's own detector (issue #4699 review). The flush deadline
        // abandons the call for offset 0, and the post-failure resync that would
        // rewind the allocator fails, so the activation keeps serving reads with
        // its next offset above the window and nothing in flight. Once the call
        // settles without landing, the window is a trailing hole: nothing is stored
        // above it, and a recovered allocator issues it again. Only the clamp at
        // highest stored + 1 keeps it unexposed.
        var provider = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var options = new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalFlushTimeout = TimeSpan.FromMilliseconds(300),
        };
        var first = await CreateGrainAsync(provider, options);
        provider.FailNextHighestRead();
        var abandoned = first.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await provider.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.That(async () => await abandoned, Throws.TypeOf<TimeoutException>());
        Assert.That(provider.HighestReadFailed.Task.IsCompleted, Is.True, "the post-failure resync ran and failed");

        provider.Fail();
        await provider.Settled.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var windows = WalAbandonedFlushRegistry.For(provider, TreeId, 0);
        var stopAt = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (windows.LowestUnsettledStart() is not null && DateTime.UtcNow < stopAt)
        {
            await Task.Delay(10);
        }

        Assert.That(windows.LowestUnsettledStart(), Is.Null, "the abandoned call has settled, so it no longer bounds the head");
        var head = await first.GetReadableHeadAsync(CancellationToken.None);
        var page = await first.ReadAsync(0L, 256, CancellationToken.None);

        var second = await CreateGrainAsync(provider, options);
        var reissued = await second.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        await WaitForOwnFlushToSettleAsync(second, reissued);
        var seen = (await second.ReadAsync(head, 256, CancellationToken.None)).Entries.Select(e => e.Sequence).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(head, Is.EqualTo(0L), "the readable head does not pass the trailing hole");
            Assert.That(page.NextSequence, Is.EqualTo(0L), "a cursor read does not advance past the trailing hole either");
            Assert.That(reissued, Is.EqualTo(0L), "the recovered allocator issues the hole's offset again");
            Assert.That(seen, Does.Contain(reissued), "a reader resuming from the head sees the reissued write");
        });
    }
    [Test]
    public async Task Quiesce_of_a_reactivated_shard_is_not_quiesced_while_its_predecessors_abandoned_flush_can_land()
    {
        // Issue #4699: the predecessor activation abandoned its call for offset 0 at
        // the flush deadline, and only the process-wide registry remembers it. Were
        // the new activation's quiesce to report a stable tail, the move's copy
        // would take {1} and the late landing at 0 would stay behind on the source.
        var provider = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0);
        var options = new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalFlushTimeout = TimeSpan.FromMilliseconds(300),
        };
        var first = await CreateGrainAsync(provider, options);
        var abandoned = first.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await provider.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await first.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        Assert.That(async () => await abandoned, Throws.TypeOf<TimeoutException>());

        var second = await CreateGrainAsync(provider, options);
        var refused = await second.QuiesceForMoveAsync(0, TimeSpan.FromMinutes(1), CancellationToken.None);

        provider.Open();
        await provider.Landed.Task.WaitAsync(TimeSpan.FromSeconds(15));
        // The provider task completes just after it signals the landing; poll the
        // quiesce until the registry sees it settled.
        var settled = await second.QuiesceForMoveAsync(0, TimeSpan.FromMinutes(1), CancellationToken.None);
        var stopAt = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (!settled.Quiesced && DateTime.UtcNow < stopAt)
        {
            await Task.Delay(20);
            settled = await second.QuiesceForMoveAsync(0, TimeSpan.FromMinutes(1), CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(refused.Quiesced, Is.False, "the predecessor's abandoned call for offset 0 can still land under the copy");
            Assert.That(refused.DrainIncomplete, Is.True);
            Assert.That(settled.Quiesced, Is.True, "once the call has settled the tail is stable");
            Assert.That(settled.HighestOffsetInclusive, Is.EqualTo(1L));
        });
    }
    [Test]
    public async Task A_resync_never_reissues_an_offset_beneath_a_successor_flush_still_in_motion()
    {
        // Issue #5799. The flush for offset 0 fails while the flush for offset 1 is
        // still in motion. The failure handler waits for the later slot's FlushAsync,
        // which times out and completes while its provider call keeps running, then
        // re-seeds the allocator from the provider's tail (nothing stored). Re-seeded
        // to 0 it reissues offset 1 to a new append, colliding with the late landing
        // the provider is about to make there. The allocator must start past it.
        var gates = new LateLandingWalStorageProvider(
            new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 0),
            gatedOffset: 1);
        var inner = (LateLandingWalStorageProvider)gates.Inner;
        var grain = await CreateGrainAsync(gates, new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalFlushTimeout = TimeSpan.FromSeconds(1),
        });

        var failed = grain.AppendAsync(MakeEntry("k0"), CancellationToken.None);
        await inner.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var successor = grain.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        await gates.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));

        // Offset 0 fails outright; offset 1's call is still running when its flush times out.
        inner.Fail();
        Assert.That(async () => await failed, Throws.Exception);
        Assert.That(async () => await successor, Throws.Exception);

        var next = await AppendOnceTheResyncSettlesAsync(grain, "k2");

        gates.Open();
        await gates.Landed.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await WaitForAbandonedWindowsToSettleAsync(gates);
        var stored = await ReadAllAsync(gates);

        Assert.Multiple(() =>
        {
            Assert.That(next, Is.GreaterThanOrEqualTo(2L),
                "the allocator was re-seeded beneath the window whose call was still in motion, so a new append reissued its offset");
            Assert.That(stored.Select(e => e.Offset), Is.EqualTo(new[] { 1L, next }),
                "the late landing and the new append both stored, each at its own offset");
            Assert.That(stored.Single(e => e.Offset == 1L).Mutation.Key, Is.EqualTo("k1"));
            Assert.That(stored.Single(e => e.Offset == next).Mutation.Key, Is.EqualTo("k2"));
        });
    }

    [Test]
    public async Task A_reactivated_shard_never_reissues_an_offset_beneath_its_predecessors_abandoned_flush()
    {
        // Issue #5799, activation recovery. The predecessor stored offset 0 and
        // abandoned its flush for offset 1 at the deadline while the call kept
        // running. A new activation on the same silo recovers from the provider's
        // tail (0) and, re-seeded at 1, would reissue the abandoned window's offset.
        var provider = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 1);
        var options = new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalFlushTimeout = TimeSpan.FromMilliseconds(300),
        };
        var first = await CreateGrainAsync(provider, options);
        Assert.That(await first.AppendAsync(MakeEntry("k0"), CancellationToken.None), Is.EqualTo(0L));
        var abandoned = first.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        await provider.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.That(async () => await abandoned, Throws.TypeOf<TimeoutException>());

        var second = await CreateGrainAsync(provider, options);
        var next = await second.AppendAsync(MakeEntry("k2"), CancellationToken.None);

        provider.Open();
        await provider.Landed.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await WaitForAbandonedWindowsToSettleAsync(provider);
        var stored = await ReadAllAsync(provider);

        Assert.Multiple(() =>
        {
            Assert.That(next, Is.EqualTo(2L),
                "the new activation re-seeded beneath its predecessor's abandoned window and reissued its offset");
            Assert.That(stored.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L, 2L }));
            Assert.That(stored.Select(e => e.Mutation.Key), Is.EqualTo(new[] { "k0", "k1", "k2" }),
                "every stored offset holds the write that was allocated it");
        });
    }

    [Test]
    public async Task A_window_that_settles_during_the_tail_read_is_still_skipped()
    {
        // Issue #5799 review: the predecessor's abandoned window for offset 1 lands
        // and settles after the recovering activation has read the tail (0) but
        // before it re-seeds. The tail missed the landing and the record has already
        // pruned the settled window, so only a snapshot taken before the read keeps
        // the allocator from re-seeding at 1, beneath the landed entry.
        var provider = new LateLandingWalStorageProvider(new InMemoryWalStorageProvider(), gatedOffset: 1);
        var options = new LatticeOptions
        {
            WalMaxBatchEntries = 1,
            WalMaxPendingBatches = 8,
            WalFlushTimeout = TimeSpan.FromMilliseconds(300),
        };
        var first = await CreateGrainAsync(provider, options);
        Assert.That(await first.AppendAsync(MakeEntry("k0"), CancellationToken.None), Is.EqualTo(0L));
        var abandoned = first.AppendAsync(MakeEntry("k1"), CancellationToken.None);
        await provider.OffsetGated.Task.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.That(async () => await abandoned, Throws.TypeOf<TimeoutException>());

        provider.LandDuringNextHighestRead();
        var second = await CreateGrainAsync(provider, options);
        Assert.That(provider.LandedDuringHighestRead, Is.True, "PRECONDITION: the window landed and settled inside the tail read");
        var next = await second.AppendAsync(MakeEntry("k2"), CancellationToken.None);

        var stored = await ReadAllAsync(provider);
        Assert.Multiple(() =>
        {
            Assert.That(next, Is.EqualTo(2L), "the allocator went back beneath a window that settled during the tail read");
            Assert.That(stored.Select(e => e.Mutation.Key), Is.EqualTo(new[] { "k0", "k1", "k2" }));
        });
    }
    [Test]
    public async Task Two_activations_issuing_the_same_offset_never_displace_the_acknowledged_write()
    {
        // Issue #5799, the cross-silo case: two activations of one shard, each
        // unaware of the other (as on two silos), recover the same tail and both
        // issue offset 0. The allocator fix cannot see across activations it does
        // not share a process with, so the store must fail closed: the second
        // write is refused and the acknowledged first one is untouched.
        var provider = new InMemoryWalStorageProvider();
        var options = new LatticeOptions { WalMaxBatchEntries = 1, WalMaxPendingBatches = 8 };
        var first = await CreateGrainAsync(provider, options);
        var second = await CreateGrainAsync(provider, options);

        var acknowledged = await first.AppendAsync(MakeEntry("k-first"), CancellationToken.None);
        Assert.That(acknowledged, Is.EqualTo(0L), "PRECONDITION: the first activation took offset 0");
        Assert.That(async () => await second.AppendAsync(MakeEntry("k-second"), CancellationToken.None),
            Throws.Exception, "the second activation's write at the same offset is refused, not stored");

        var stored = await ReadAllAsync(provider);
        Assert.Multiple(() =>
        {
            Assert.That(stored.Select(e => e.Offset), Is.EqualTo(new[] { 0L }));
            Assert.That(stored.Single().Mutation.Key, Is.EqualTo("k-first"),
                "the acknowledged write is the one stored at its offset");
        });
    }
    /// <summary>
    /// Appends <paramref name="key"/> once the post-failure resync has cleared the
    /// shard's sticky failure (issue #5799): until then every append rethrows it.
    /// Fails rather than waiting forever.
    /// </summary>
    private static async Task<long> AppendOnceTheResyncSettlesAsync(WalShardGrain grain, string key)
    {
        var stopAt = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (true)
        {
            try
            {
                return await grain.AppendAsync(MakeEntry(key), CancellationToken.None);
            }
            catch (Exception) when (DateTime.UtcNow < stopAt)
            {
                await Task.Delay(20);
            }
        }
    }

    private static async Task<List<WalEntry>> ReadAllAsync(IWalStorageProvider provider)
    {
        var stored = new List<WalEntry>();
        await foreach (var entry in provider.ReadAsync(TreeId, 0, -1L, 256, CancellationToken.None))
        {
            stored.Add(entry);
        }

        return stored;
    }
    /// <summary>
    /// Waits until the flush of an append the test just awaited has left the shard's
    /// in-flight chain, the precondition for reading it back (#5796).
    /// <c>FlushAsync</c> completes the append's acknowledgement before its <c>finally</c>
    /// removes the slot from the chain, and the readable head stops at the oldest slot
    /// still in it. Under a grain scheduler the two run in one turn; this harness has no
    /// scheduler, so the acknowledgement's continuation can read in between. Fails rather
    /// than reading early.
    /// </summary>
    private static async Task WaitForOwnFlushToSettleAsync(WalShardGrain grain, long appended)
    {
        var stopAt = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        var head = await grain.GetReadableHeadAsync(CancellationToken.None);
        while (head <= appended && DateTime.UtcNow < stopAt)
        {
            await Task.Delay(10);
            head = await grain.GetReadableHeadAsync(CancellationToken.None);
        }

        Assert.That(head, Is.GreaterThan(appended), "precondition: the append's flush has left the in-flight chain");
    }
    /// <summary>
    /// Waits until no abandoned flush of the shard is outstanding: the precondition for
    /// a read to see past it (#5730). <see cref="LateLandingWalStorageProvider.Landed"/>
    /// and <c>Settled</c> fire inside the provider call, before its task completes, and
    /// the read bound lifts only on that completion. Fails rather than reading early.
    /// </summary>
    private static async Task WaitForAbandonedWindowsToSettleAsync(IWalStorageProvider provider)
    {
        var windows = WalAbandonedFlushRegistry.For(provider, TreeId, 0);
        var stopAt = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (windows.LowestUnsettledStart() is not null && DateTime.UtcNow < stopAt)
        {
            await Task.Delay(10);
        }

        Assert.That(windows.LowestUnsettledStart(), Is.Null, "precondition: the abandoned call has settled");
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> decorator whose call for one offset ignores
    /// cancellation and waits for the test, then either lands or fails: an abandoned
    /// provider call that is still in motion after its caller stopped waiting.
    /// </summary>
    private sealed class LateLandingWalStorageProvider(IWalStorageProvider inner, long gatedOffset) : IWalStorageProvider
    {
        /// <summary>The provider this one decorates, so two gates can be chained.</summary>
        internal IWalStorageProvider Inner => inner;

        private readonly TaskCompletionSource<bool> _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int _gatedOnce;

        private int _failHighest;

        internal TaskCompletionSource OffsetGated { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource Landed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource Settled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Open() => _release.TrySetResult(true);

        public void Fail() => _release.TrySetResult(false);

        public async Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            if (entries.Any(e => e.Offset == gatedOffset) && Interlocked.Exchange(ref _gatedOnce, 1) == 0)
            {
                OffsetGated.TrySetResult();
                var land = await _release.Task.ConfigureAwait(false);
                try
                {
                    if (!land)
                    {
                        throw new IOException("the abandoned provider call failed without writing");
                    }

                    await inner.AppendBatchAsync(treeId, shardIndex, entries, CancellationToken.None).ConfigureAwait(false);
                    Landed.TrySetResult();
                    return;
                }
                finally
                {
                    Settled.TrySetResult();
                }
            }

            await inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken).ConfigureAwait(false);
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        /// <summary>
        /// Makes the next <see cref="GetHighestOffsetAsync"/> call throw, then signals
        /// <see cref="HighestReadFailed"/>: a failing post-failure resync, which reads
        /// the provider's highest offset to rewind the allocator.
        /// </summary>
        public void FailNextHighestRead() => Volatile.Write(ref _failHighest, 1);

        internal TaskCompletionSource HighestReadFailed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            if (Interlocked.Exchange(ref _failHighest, 0) == 1)
            {
                HighestReadFailed.TrySetResult();
                throw new IOException("the resync's tail read failed");
            }

            var highest = await inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(false);
            if (Interlocked.Exchange(ref _landDuringHighest, 0) == 1)
            {
                // The gated call lands and settles after the tail was read, which is
                // then returned stale (issue #5799).
                Open();
                await Landed.Task.WaitAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
                var windows = WalAbandonedFlushRegistry.For(this, treeId, shardIndex);
                var stopAt = DateTime.UtcNow + TimeSpan.FromSeconds(15);
                while (windows.LowestUnsettledStart() is not null && DateTime.UtcNow < stopAt)
                {
                    await Task.Delay(10).ConfigureAwait(false);
                }

                LandedDuringHighestRead = windows.LowestUnsettledStart() is null;
            }

            return highest;
        }

        private int _landDuringHighest;

        /// <summary>Makes the next tail read release the gated call and wait for it to settle before returning.</summary>
        public void LandDuringNextHighestRead() => Volatile.Write(ref _landDuringHighest, 1);

        /// <summary>Whether a tail read released the gated call and saw it settle.</summary>
        internal bool LandedDuringHighestRead { get; private set; }
        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }
}
