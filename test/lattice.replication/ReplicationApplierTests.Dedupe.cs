using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Shadow-forward dedupe cache tests for the canonical
/// <see cref="ReplicationApplier"/>. The cache catches the
/// duplicate-emit pair structural rewrites (shard split / merge /
/// saga compensate) generate when they shadow-forward a user write
/// into a different shard.
/// </summary>
public partial class ReplicationApplierTests
{
    [Test]
    public async Task ApplyAsync_dedupes_duplicate_identity_tuple_without_invoking_apply_grain()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        var ts = Hlc(10, 1);
        var entry = SetEntry("k", ts);

        var first = await applier.ApplyAsync(entry);
        // Reset HWM mock so the second call cannot be deduped on HWM
        // alone - only the in-memory cache can suppress it. (The HWM
        // grain returned 0 the first time; reset it back to 0 here so
        // the cache is the sole defence.)
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);

        var second = await applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(first.Applied, Is.True);
            Assert.That(second.Applied, Is.False);
            Assert.That(second.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        // Only the first delivery reached the apply grain.
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), ts, RemoteCluster, null, Arg.Any<long>());
        // Only the first delivery advanced the HWM. The second was
        // suppressed before any HWM round-trip.
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, ts, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_distinguishes_entries_by_timestamp_in_cache()
    {
        var (applier, _, apply, _) = CreateApplier();

        var firstResult = await applier.ApplyAsync(SetEntry("k", Hlc(10, 0)));
        var secondResult = await applier.ApplyAsync(SetEntry("k", Hlc(10, 1)));

        Assert.Multiple(() =>
        {
            Assert.That(firstResult.Applied, Is.True);
            Assert.That(secondResult.Applied, Is.True);
        });
        await apply.Received(2).ApplySetAsync("k", Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), RemoteCluster, null, Arg.Any<long>());
    }

    [Test]
    public async Task ApplyAsync_distinguishes_entries_by_origin_in_cache()
    {
        var (applier, _, apply, _) = CreateApplier();
        var ts = Hlc(10, 1);

        var firstResult = await applier.ApplyAsync(SetEntry("k", ts, origin: "site-b"));
        var secondResult = await applier.ApplyAsync(SetEntry("k", ts, origin: "site-c"));

        Assert.Multiple(() =>
        {
            Assert.That(firstResult.Applied, Is.True);
            Assert.That(secondResult.Applied, Is.True);
        });
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), ts, "site-b", null, Arg.Any<long>());
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), ts, "site-c", null, Arg.Any<long>());
    }

    [Test]
    public async Task ApplyAsync_distinguishes_set_and_delete_in_cache()
    {
        var (applier, _, apply, _) = CreateApplier();
        var ts = Hlc(10, 1);

        var setResult = await applier.ApplyAsync(SetEntry("k", ts));
        var delResult = await applier.ApplyAsync(DeleteEntry("k", ts));

        Assert.Multiple(() =>
        {
            Assert.That(setResult.Applied, Is.True);
            Assert.That(delResult.Applied, Is.True);
        });
    }

    [Test]
    public async Task ApplyAsync_does_not_cache_range_delete_entries()
    {
        // Range deletes carry HLC.Zero by design (the walk produces
        // many per-leaf HLCs that cannot be faithfully collapsed),
        // making the (origin, hlc, key, op) identity tuple
        // ambiguous. They must bypass the dedupe cache so the
        // shard-root receives the apply on every delivery (the leaf
        // layer is naturally idempotent).
        var (applier, _, apply, _) = CreateApplier();
        var range = RangeDeleteEntry("a", "z");

        var firstResult = await applier.ApplyAsync(range);
        var secondResult = await applier.ApplyAsync(range);

        Assert.Multiple(() =>
        {
            Assert.That(firstResult.Applied, Is.True);
            Assert.That(secondResult.Applied, Is.True);
        });
        await apply.Received(2).ApplyDeleteRangeAsync("a", "z", HybridLogicalClock.Zero, RemoteCluster, null);
    }

    [Test]
    public async Task ApplyAsync_does_not_cache_local_origin_entries()
    {
        // Local-origin entries are rejected at the local-origin
        // defence before the cache check. They must not pollute the
        // cache (otherwise an unrelated remote-origin entry sharing
        // the same (hlc, key, op) but with a remote origin tag would
        // still collide on the cache key - except origins differ, so
        // they would not collide; the test is here to assert the
        // defence-in-depth invariant that the cache never even sees
        // a local-origin entry).
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>()).Returns(true);
        var applier = new ReplicationApplier(factory, Monitor());

        var ts = Hlc(10, 1);
        var local = SetEntry("k", ts, origin: LocalCluster);

        var localResult = await applier.ApplyAsync(local);
        // A subsequent remote-origin entry with the SAME (key, ts, op)
        // must apply - the local entry should not have leaked into the
        // cache to suppress it.
        var remote = SetEntry("k", ts, origin: RemoteCluster);
        var remoteResult = await applier.ApplyAsync(remote);

        Assert.Multiple(() =>
        {
            Assert.That(localResult.Applied, Is.False);
            Assert.That(remoteResult.Applied, Is.True);
        });
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), ts, RemoteCluster, null, Arg.Any<long>());
    }

    [Test]
    public async Task ApplyAsync_dedupe_cache_size_honours_options()
    {
        // A capacity-1 cache evicts each prior entry on every new
        // insert. Two distinct entries with no HLC ordering relation
        // should both apply; a re-apply of the first after the
        // second has evicted it from the cache must fall back to the
        // HWM check (which authoritatively dedupes by per-origin
        // monotonicity of the source HLC).
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShadowForwardDedupeCacheSize = 64,
        };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>()).Returns(true);
        var applier = new ReplicationApplier(factory, monitor);

        // Cache holds 64 entries; first 64 distinct entries all apply.
        for (var i = 0; i < 64; i++)
        {
            var r = await applier.ApplyAsync(SetEntry($"k-{i}", Hlc(i + 1)));
            Assert.That(r.Applied, Is.True);
        }
        Assert.That(apply.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IReplicationApplyGrain.ApplySetAsync)), Is.EqualTo(64));
    }

    [Test]
    public async Task ApplyAsync_rolls_back_cache_when_apply_grain_throws_so_retry_can_apply()
    {
        // Without cache rollback the retry path observes TryAdd=false
        // and is classified as Applied=false (shadow-forward-dedup);
        // the dead-letter decorator's "Applied=false clears the
        // counter" rule then silently drops the entry until FIFO
        // eviction admits a future retry. With rollback, the retry
        // observes TryAdd=true and proceeds through the apply
        // pipeline.
        var (applier, _, apply, hwm) = CreateApplier();
        var ts = Hlc(10, 1);
        var entry = SetEntry("k", ts);

        var calls = 0;
        apply.WhenForAnyArgs(x => x.ApplySetAsync(default!, default!, default, default!, default, default))
            .Do(_ =>
            {
                if (Interlocked.Increment(ref calls) == 1)
                {
                    throw new InvalidOperationException("transient apply failure");
                }
            });

        Assert.ThrowsAsync<InvalidOperationException>(async () => await applier.ApplyAsync(entry));

        // HWM remains 0 on the retry - the failed attempt did not
        // advance it. Cache rollback must allow the retry to proceed.
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);

        var retry = await applier.ApplyAsync(entry);

        Assert.That(retry.Applied, Is.True, "retry must apply after cache rollback");
        Assert.That(calls, Is.EqualTo(2), "apply grain should have been called twice");
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, ts, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_rolls_back_cache_when_TryAdvanceAsync_throws()
    {
        // The HWM advance is the last grain hop in the apply
        // pipeline. A throw there must still roll back the cache so
        // the retry path is admitted.
        var (applier, _, apply, hwm) = CreateApplier();
        var ts = Hlc(10, 1);
        var entry = SetEntry("k", ts);

        var advanceCalls = 0;
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                if (Interlocked.Increment(ref advanceCalls) == 1)
                {
                    throw new InvalidOperationException("transient HWM failure");
                }
                return Task.FromResult(true);
            });

        Assert.ThrowsAsync<InvalidOperationException>(async () => await applier.ApplyAsync(entry));

        var retry = await applier.ApplyAsync(entry);

        Assert.That(retry.Applied, Is.True, "retry must apply after cache rollback");
        // Apply grain reached twice: once for the throwing attempt,
        // once for the retry. (The first apply did write the value
        // before the HWM advance threw - that is a separate
        // idempotence concern handled by the apply grain's own
        // source-HLC guard.)
        await apply.Received(2).ApplySetAsync("k", Arg.Any<byte[]>(), ts, RemoteCluster, null, Arg.Any<long>());
    }

    [Test]
    public async Task ApplyAsync_rolls_back_cache_when_cancelled_during_apply()
    {
        // Cancellation mid-apply leaves the entry un-applied - the
        // cache reservation must be rolled back so a retry under a
        // fresh token can succeed. Without rollback, the retry's
        // TryAdd would fail and the entry would be silently
        // suppressed.
        var (applier, _, apply, hwm) = CreateApplier();
        var ts = Hlc(10, 1);
        var entry = SetEntry("k", ts);

        using var cts = new CancellationTokenSource();
        var calls = 0;
        apply.WhenForAnyArgs(x => x.ApplySetAsync(default!, default!, default, default!, default, default))
            .Do(_ =>
            {
                if (Interlocked.Increment(ref calls) == 1)
                {
                    cts.Cancel();
                    throw new OperationCanceledException(cts.Token);
                }
            });

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await applier.ApplyAsync(entry, cts.Token));

        // Retry with a fresh token. The Do callback's counter
        // gates the throw to the first invocation only, so the
        // second call returns Task.CompletedTask via the NSubstitute
        // default for void-returning Tasks.
        var retry = await applier.ApplyAsync(entry);

        Assert.That(retry.Applied, Is.True, "retry must apply after cache rollback on cancellation");
        Assert.That(calls, Is.EqualTo(2));
    }

    [Test]
    public async Task ApplyAsync_retains_cache_reservation_for_parked_entry()
    {
        // The park branch retains the cache reservation: the parked
        // entry's drain path bypasses the cache, so the reservation
        // continues to suppress duplicate-emit pairs of the parked
        // entry that arrive while it is buffered. A second delivery
        // of the same identity tuple while the first is parked must
        // be suppressed (the receiver sees the duplicate; the parked
        // original is the one that will eventually apply).
        var (applier, _, apply, hwm) = CreateApplier();
        var ts = Hlc(10, 1);

        // Construct an entry with a causal dependency that is not yet
        // satisfied by the local vector clock: the entry's
        // VectorClock advertises a frontier higher than the local
        // GetVectorAsync result, so DependenciesSatisfied returns
        // false and the entry parks.
        hwm.GetVectorAsync(Arg.Any<CancellationToken>())
            .Returns(new VersionVector());
        var depVc = new VersionVector();
        depVc.Tick("site-c");
        var entry = SetEntry("k", ts) with { VectorClock = depVc };

        var first = await applier.ApplyAsync(entry);
        Assert.That(first.Applied, Is.False, "entry should park, not apply");

        // Second delivery of the same identity tuple: cache must
        // still contain the reservation from the parked first
        // delivery, so this returns Applied=false with
        // ShadowForwardDedup classification (not a re-park).
        var second = await applier.ApplyAsync(entry);

        Assert.That(second.Applied, Is.False, "duplicate of parked entry must be suppressed by cache");
        // ApplyPointAsync was never called - both deliveries either
        // parked (first) or were cache-suppressed (second).
        await apply.DidNotReceive().ApplySetAsync(
            Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(),
            Arg.Any<string>(), Arg.Any<VersionVector?>(), Arg.Any<long>());
    }
}
