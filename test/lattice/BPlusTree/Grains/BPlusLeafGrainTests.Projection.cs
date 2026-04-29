using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the dormant <see cref="ILeafProjection"/> seam on
/// <see cref="BPlusLeafGrain"/>.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static ILeafProjection AsProjection(BPlusLeafGrain grain) => grain;

    private static LatticeMutation BuildSet(
        string key,
        byte[] value,
        long hlcPhysical = 100,
        long hlcLogical = 0,
        long expiresAtTicks = 0,
        string? originClusterId = null,
        VersionVector? vectorClock = null,
        string treeId = "tree-projection")
        => new()
        {
            TreeId = treeId,
            Kind = MutationKind.Set,
            Key = key,
            Value = value,
            Timestamp = new HybridLogicalClock { WallClockTicks = hlcPhysical, Counter = (int)hlcLogical },
            IsTombstone = false,
            ExpiresAtTicks = expiresAtTicks,
            OriginClusterId = originClusterId,
            VectorClock = vectorClock,
        };

    private static LatticeMutation BuildDelete(
        string key,
        long hlcPhysical = 200,
        long hlcLogical = 0,
        string treeId = "tree-projection")
        => new()
        {
            TreeId = treeId,
            Kind = MutationKind.Delete,
            Key = key,
            Timestamp = new HybridLogicalClock { WallClockTicks = hlcPhysical, Counter = (int)hlcLogical },
            IsTombstone = true,
        };

    private static LatticeMutation BuildDeleteRange(
        string startInclusive,
        string endExclusive,
        long hlcPhysical = 300,
        long hlcLogical = 0,
        string treeId = "tree-projection")
        => new()
        {
            TreeId = treeId,
            Kind = MutationKind.DeleteRange,
            Key = startInclusive,
            EndExclusiveKey = endExclusive,
            Timestamp = new HybridLogicalClock { WallClockTicks = hlcPhysical, Counter = (int)hlcLogical },
            IsTombstone = true,
        };

    [Test]
    public async Task Apply_set_inserts_live_entry_into_in_memory_projection()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1")));

        var read = await grain.GetAsync("k1");
        Assert.That(read, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("v1"));
    }

    [Test]
    public void Apply_set_does_not_persist_state()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1")));

        Assert.That(state.WriteCount, Is.Zero);
    }

    [Test]
    public async Task Apply_set_with_expiry_preserves_expires_at_ticks()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        const long expiresAt = 9_000_000_000L;
        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), expiresAtTicks: expiresAt));

        var raw = await grain.GetRawEntryAsync("k1");
        Assert.That(raw, Is.Not.Null);
        Assert.That(raw!.Value.ExpiresAtTicks, Is.EqualTo(expiresAt));
    }

    [Test]
    public async Task Apply_set_preserves_origin_cluster_id_and_vector_clock()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        var vc = new VersionVector();
        vc.Tick("dc-1");

        projection.Apply(BuildSet(
            "k1",
            Encoding.UTF8.GetBytes("v1"),
            originClusterId: "dc-1",
            vectorClock: vc));

        var raw = await grain.GetRawEntryAsync("k1");
        Assert.That(raw, Is.Not.Null);
        Assert.That(raw!.Value.OriginClusterId, Is.EqualTo("dc-1"));
        Assert.That(raw.Value.VectorClock, Is.SameAs(vc));
    }

    [Test]
    public async Task Apply_set_uses_LWW_so_higher_timestamp_wins()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("old"), hlcPhysical: 100));
        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("new"), hlcPhysical: 200));

        var read = await grain.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("new"));
    }

    [Test]
    public async Task Apply_set_uses_LWW_so_lower_timestamp_loses()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("new"), hlcPhysical: 200));
        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("old"), hlcPhysical: 100));

        var read = await grain.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("new"));
    }

    [Test]
    public async Task Apply_set_is_idempotent_under_replay()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        var mutation = BuildSet("k1", Encoding.UTF8.GetBytes("v1"));
        projection.Apply(mutation);
        projection.Apply(mutation);
        projection.Apply(mutation);

        var read = await grain.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("v1"));
    }

    [Test]
    public async Task Apply_set_is_commutative_under_reordering()
    {
        var grainA = CreateGrain();
        var grainB = CreateGrain();
        var a = AsProjection(grainA);
        var b = AsProjection(grainB);

        var m1 = BuildSet("k1", Encoding.UTF8.GetBytes("a"), hlcPhysical: 100);
        var m2 = BuildSet("k1", Encoding.UTF8.GetBytes("b"), hlcPhysical: 200);

        a.Apply(m1);
        a.Apply(m2);

        b.Apply(m2);
        b.Apply(m1);

        Assert.That(await grainA.GetAsync("k1"), Is.EqualTo(await grainB.GetAsync("k1")));
    }

    [Test]
    public async Task Apply_delete_tombstones_the_key()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        projection.Apply(BuildDelete("k1", hlcPhysical: 200));

        var read = await grain.GetAsync("k1");
        Assert.That(read, Is.Null);
    }

    [Test]
    public async Task Apply_delete_loses_LWW_to_higher_set_timestamp()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildDelete("k1", hlcPhysical: 100));
        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("after"), hlcPhysical: 200));

        var read = await grain.GetAsync("k1");
        Assert.That(read, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("after"));
    }

    [Test]
    public async Task Apply_delete_range_tombstones_keys_inside_range()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("a", Encoding.UTF8.GetBytes("a"), hlcPhysical: 100));
        projection.Apply(BuildSet("b", Encoding.UTF8.GetBytes("b"), hlcPhysical: 100));
        projection.Apply(BuildSet("c", Encoding.UTF8.GetBytes("c"), hlcPhysical: 100));
        projection.Apply(BuildSet("d", Encoding.UTF8.GetBytes("d"), hlcPhysical: 100));

        projection.Apply(BuildDeleteRange("b", "d", hlcPhysical: 200));

        Assert.That(await grain.GetAsync("a"), Is.Not.Null);
        Assert.That(await grain.GetAsync("b"), Is.Null);
        Assert.That(await grain.GetAsync("c"), Is.Null);
        Assert.That(await grain.GetAsync("d"), Is.Not.Null, "d is excluded by half-open range bound");
    }

    [Test]
    public async Task Apply_delete_range_with_no_matching_keys_is_noop()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("a", Encoding.UTF8.GetBytes("a"), hlcPhysical: 100));
        projection.Apply(BuildDeleteRange("m", "z", hlcPhysical: 200));

        Assert.That(await grain.GetAsync("a"), Is.Not.Null);
    }

    [Test]
    public async Task Apply_delete_range_with_inverted_bounds_is_noop()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("a", Encoding.UTF8.GetBytes("a"), hlcPhysical: 100));
        projection.Apply(BuildDeleteRange("z", "a", hlcPhysical: 200));

        Assert.That(await grain.GetAsync("a"), Is.Not.Null);
    }

    [Test]
    public async Task Apply_delete_range_with_null_end_is_noop()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("a", Encoding.UTF8.GetBytes("a"), hlcPhysical: 100));
        projection.Apply(new LatticeMutation
        {
            TreeId = "tree-projection",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = null,
            Timestamp = new HybridLogicalClock { WallClockTicks = 200 },
            IsTombstone = true,
        });

        Assert.That(await grain.GetAsync("a"), Is.Not.Null);
    }

    [Test]
    public void Apply_unknown_kind_throws()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        var bogus = new LatticeMutation
        {
            TreeId = "tree-projection",
            Kind = (MutationKind)99,
            Key = "k1",
            Timestamp = new HybridLogicalClock { WallClockTicks = 100 },
        };

        Assert.That(() => projection.Apply(bogus), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task GetCheckpointOffset_defaults_to_zero()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        var offset = await projection.GetCheckpointOffsetAsync();

        Assert.That(offset, Is.Zero);
    }

    [Test]
    public async Task SetCheckpointOffset_persists_offset_and_writes_state()
    {
        var state = new FakePersistentState<LeafNodeState>();
        // Every-entry mode forces an immediate durable persist on every
        // advance, restoring the immediate-persist contract for callers
        // that opt in.
        var options = new LatticeOptions { MaterialiserCheckpointInterval = TimeSpan.Zero };
        var grain = CreateGrain(state, options: options);
        var projection = AsProjection(grain);

        await projection.SetCheckpointOffsetAsync(42);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(42));
        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(await projection.GetCheckpointOffsetAsync(), Is.EqualTo(42));
    }

    [Test]
    public async Task SetCheckpointOffset_advances_monotonically()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        await projection.SetCheckpointOffsetAsync(10);
        await projection.SetCheckpointOffsetAsync(20);
        await projection.SetCheckpointOffsetAsync(30);

        Assert.That(await projection.GetCheckpointOffsetAsync(), Is.EqualTo(30));
    }

    [Test]
    public async Task SetCheckpointOffset_rejects_backwards_advance()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        // Advance via every-entry mode so the persisted offset is 50;
        // the backwards-rejection invariant must still hold whether the
        // current view is durable or pending.
        await ((ILeafProjection)grain).FlushCheckpointAsync();
        await projection.SetCheckpointOffsetAsync(50);
        await ((ILeafProjection)grain).FlushCheckpointAsync();

        Assert.That(
            async () => await projection.SetCheckpointOffsetAsync(49),
            Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(await projection.GetCheckpointOffsetAsync(), Is.EqualTo(50));
    }

    [Test]
    public async Task SetCheckpointOffset_idempotent_advance_persists_in_memory_apply_changes()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var options = new LatticeOptions { MaterialiserCheckpointInterval = TimeSpan.Zero };
        var grain = CreateGrain(state, options: options);
        var projection = AsProjection(grain);

        await projection.SetCheckpointOffsetAsync(10);
        var firstWriteCount = state.WriteCount;

        // Apply mutates only in-memory state and does not persist.
        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1")));
        Assert.That(state.WriteCount, Is.EqualTo(firstWriteCount));

        // Re-asserting the same checkpoint flushes pending in-memory work.
        await projection.SetCheckpointOffsetAsync(10);
        Assert.That(state.WriteCount, Is.EqualTo(firstWriteCount + 1));
        Assert.That(state.State.Entries.ContainsKey("k1"), Is.True);
    }

    // --- Materialiser checkpoint coalescing predicate ---

    [Test]
    public async Task SetCheckpointOffset_coalesces_under_default_options_and_does_not_persist_immediately()
    {
        // Default MaterialiserCheckpointEntries=1000, Interval=1s.
        // A single small advance hits neither threshold and must not
        // touch durable storage on the hot path.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var projection = AsProjection(grain);

        await projection.SetCheckpointOffsetAsync(42);

        Assert.That(state.WriteCount, Is.Zero);
        Assert.That(state.State.ProjectionCheckpointOffset, Is.Zero);
        // GetCheckpointOffsetAsync reflects the most-recent advance.
        Assert.That(await projection.GetCheckpointOffsetAsync(), Is.EqualTo(42));
    }

    [Test]
    public async Task SetCheckpointOffset_persists_when_entries_threshold_met()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var options = new LatticeOptions
        {
            MaterialiserCheckpointEntries = 5,
            MaterialiserCheckpointInterval = Timeout.InfiniteTimeSpan,
        };
        var grain = CreateGrain(state, options: options);
        var projection = AsProjection(grain);

        // Advances under the threshold accumulate in memory.
        await projection.SetCheckpointOffsetAsync(1);
        await projection.SetCheckpointOffsetAsync(2);
        await projection.SetCheckpointOffsetAsync(4);
        Assert.That(state.WriteCount, Is.Zero);
        Assert.That(state.State.ProjectionCheckpointOffset, Is.Zero);

        // Crossing the threshold (5 entries pending) flushes durably.
        await projection.SetCheckpointOffsetAsync(5);
        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(5));
    }

    [Test]
    public async Task SetCheckpointOffset_persists_immediately_when_interval_is_zero()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var options = new LatticeOptions { MaterialiserCheckpointInterval = TimeSpan.Zero };
        var grain = CreateGrain(state, options: options);
        var projection = AsProjection(grain);

        await projection.SetCheckpointOffsetAsync(1);
        await projection.SetCheckpointOffsetAsync(2);
        await projection.SetCheckpointOffsetAsync(3);

        Assert.That(state.WriteCount, Is.EqualTo(3));
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3));
    }

    [Test]
    public async Task SetCheckpointOffset_rejects_backwards_against_pending_offset()
    {
        // Backwards-rejection must compare against the pending in-memory
        // offset, not just the persisted one. With default coalescing,
        // a fresh advance to 100 stays in-memory; an attempt to roll
        // back to 50 must still throw even though the persisted offset
        // is 0.
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        await projection.SetCheckpointOffsetAsync(100);

        Assert.That(
            async () => await projection.SetCheckpointOffsetAsync(50),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task FlushCheckpoint_persists_pending_offset()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var projection = AsProjection(grain);

        await projection.SetCheckpointOffsetAsync(42);
        Assert.That(state.WriteCount, Is.Zero);

        await projection.FlushCheckpointAsync();

        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(42));
    }

    [Test]
    public async Task FlushCheckpoint_is_noop_when_no_advance_pending()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var projection = AsProjection(grain);

        await projection.FlushCheckpointAsync();
        await projection.FlushCheckpointAsync();

        Assert.That(state.WriteCount, Is.Zero);
    }

    [Test]
    public void FlushCheckpoint_throws_on_pre_cancelled_token()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await projection.FlushCheckpointAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task OnDeactivateAsync_flushes_pending_checkpoint()
    {
        // Graceful deactivation must persist any unflushed advance so a
        // clean shutdown does not lose progress the materialiser has
        // already issued.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var projection = AsProjection(grain);

        await projection.SetCheckpointOffsetAsync(99);
        Assert.That(state.WriteCount, Is.Zero);

        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(99));
    }

    [Test]
    public void GetCheckpointOffset_throws_on_pre_cancelled_token()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await projection.GetCheckpointOffsetAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void SetCheckpointOffset_throws_on_pre_cancelled_token()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await projection.SetCheckpointOffsetAsync(1, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Replay_from_zero_against_empty_projection_matches_foreground_writes()
    {
        // Capture observer payloads from a foreground-write grain, then
        // replay the same payloads against a fresh grain via Apply. The
        // post-replay leaf must observe the same key-set + values as the
        // foreground-write grain.
        var observer = new RecordingMutationObserver();
        var foreground = CreateGrainWithObserver(observer, treeId: "tree-replay");
        await foreground.SetAsync("a", Encoding.UTF8.GetBytes("alpha"));
        await foreground.SetAsync("b", Encoding.UTF8.GetBytes("beta"));
        await foreground.SetAsync("c", Encoding.UTF8.GetBytes("gamma"));
        await foreground.DeleteAsync("b");

        var captured = observer.Mutations;
        Assert.That(captured, Has.Count.EqualTo(4));

        var replay = CreateGrain();
        var projection = AsProjection(replay);
        foreach (var m in captured)
            projection.Apply(m);

        Assert.That(Encoding.UTF8.GetString((await replay.GetAsync("a"))!), Is.EqualTo("alpha"));
        Assert.That(await replay.GetAsync("b"), Is.Null);
        Assert.That(Encoding.UTF8.GetString((await replay.GetAsync("c"))!), Is.EqualTo("gamma"));
    }

    [Test]
    public async Task Replay_with_set_delete_and_delete_range_matches_foreground_writes()
    {
        // Acceptance for the projection rebuild seam: a deterministic Apply
        // replay over a non-trivial batch of mutations must produce a
        // projection equivalent to the foreground-write grain. Exercises
        // all three MutationKind paths (Set, Delete, DeleteRange) at scale.
        // Leaf-level DeleteRangeAsync is observer-silent by design (range
        // events fire at the shard level), so the range delete is
        // synthesized as a LatticeMutation directly rather than captured.
        // Entry count stays under the default MaxLeafKeys so the foreground
        // grain does not trip the split path in unit-test mode.
        const int N = 100;
        const string treeId = "tree-replay-scale";
        var observer = new RecordingMutationObserver();
        var foreground = CreateGrainWithObserver(observer, treeId: treeId);

        for (int i = 0; i < N; i++)
            await foreground.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));
        await foreground.DeleteAsync("k-0010");
        await foreground.DeleteAsync("k-0050");

        var captured = observer.Mutations.ToList();
        Assert.That(captured, Has.Count.EqualTo(N + 2));

        // Synthesize the range-delete mutation with an HLC strictly greater
        // than every captured Set/Delete so it wins LWW deterministically.
        var rangeDelete = BuildDeleteRange(
            startInclusive: "k-0020",
            endExclusive: "k-0030",
            hlcPhysical: long.MaxValue / 2,
            treeId: treeId);

        var replay = CreateGrain();
        var projection = AsProjection(replay);
        foreach (var m in captured)
            projection.Apply(m);
        projection.Apply(rangeDelete);

        // Spot-check: deleted singletons gone, range survivors gone, others present.
        Assert.That(await replay.GetAsync("k-0000"), Is.Not.Null);
        Assert.That(await replay.GetAsync("k-0010"), Is.Null);
        Assert.That(await replay.GetAsync("k-0019"), Is.Not.Null);
        Assert.That(await replay.GetAsync("k-0020"), Is.Null);
        Assert.That(await replay.GetAsync("k-0025"), Is.Null);
        Assert.That(await replay.GetAsync("k-0029"), Is.Null);
        Assert.That(await replay.GetAsync("k-0030"), Is.Not.Null);
        Assert.That(await replay.GetAsync("k-0050"), Is.Null);
        Assert.That(await replay.GetAsync("k-0099"), Is.Not.Null);

        // Tighter cross-check: total live key count.
        // 100 sets - 2 singleton deletes - 10 range deletes = 88.
        int liveCount = 0;
        for (int i = 0; i < N; i++)
        {
            if (await replay.GetAsync($"k-{i:D4}") is not null)
                liveCount++;
        }
        Assert.That(liveCount, Is.EqualTo(88));
    }
}
