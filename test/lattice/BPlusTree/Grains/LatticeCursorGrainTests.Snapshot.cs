using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the snapshot-cursor (zero-observable-writes)
/// orchestration partial of <see cref="LatticeCursorGrain"/>. Covers
/// argument validation on the new <c>OpenSnapshotAsync</c> seam,
/// persisted-state shape, idempotent re-open, and the snapshot-mode
/// dispatch decisions on <c>Next*Async</c>. Per-shard fan-out and
/// k-way merging are covered by integration tests against a real
/// snapshot leaf; this fixture validates only the cursor-grain-side
/// contract.
/// </summary>
public partial class LatticeCursorGrainTests
{
    private static LatticeSnapshotCoordinate MakeCoordinate(
        long treeMapVersion = 1,
        params (int shard, long offset)[] shards)
    {
        var dict = new Dictionary<int, long>();
        foreach (var (s, o) in shards) dict[s] = o;
        return new LatticeSnapshotCoordinate(
            treeMapVersion,
            dict,
            HybridLogicalClock.Zero);
    }

    [Test]
    public void OpenSnapshotAsync_throws_on_null_treeId()
    {
        var (grain, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.OpenSnapshotAsync(
                null!,
                new LatticeCursorSpec { Kind = LatticeCursorKind.Keys, ZeroObservableWrites = true },
                MakeCoordinate(shards: (0, 0))));
    }

    [Test]
    public void OpenSnapshotAsync_throws_when_ZeroObservableWrites_is_false()
    {
        var (grain, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.OpenSnapshotAsync(
                TreeId,
                new LatticeCursorSpec { Kind = LatticeCursorKind.Keys, ZeroObservableWrites = false },
                MakeCoordinate(shards: (0, 0))));
    }

    [Test]
    public void OpenSnapshotAsync_throws_for_DeleteRange_spec()
    {
        var (grain, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentException>(
            () => grain.OpenSnapshotAsync(
                TreeId,
                new LatticeCursorSpec
                {
                    Kind = LatticeCursorKind.DeleteRange,
                    StartInclusive = "a",
                    EndExclusive = "z",
                    ZeroObservableWrites = true,
                },
                MakeCoordinate(shards: (0, 0))));
    }

    [Test]
    public async Task OpenSnapshotAsync_persists_coordinate_and_marks_open()
    {
        var (grain, state, _) = CreateGrain();
        var coordinate = MakeCoordinate(treeMapVersion: 7, (0, 42), (1, 99));

        await grain.OpenSnapshotAsync(
            TreeId,
            new LatticeCursorSpec { Kind = LatticeCursorKind.Keys, ZeroObservableWrites = true },
            coordinate);

        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Open));
        Assert.That(state.State.TreeId, Is.EqualTo(TreeId));
        Assert.That(state.State.Spec.ZeroObservableWrites, Is.True);
        Assert.That(state.State.SnapshotCoordinate, Is.Not.Null);
        Assert.That(state.State.SnapshotCoordinate!.Value.TreeMapVersion, Is.EqualTo(7));
        Assert.That(state.State.SnapshotCoordinate.Value.PerShardWalOffsets.Count, Is.EqualTo(2));
        Assert.That(state.State.SnapshotPinId, Is.Not.EqualTo(Guid.Empty));
    }

    [Test]
    public async Task OpenSnapshotAsync_idempotent_for_same_arguments()
    {
        var (grain, state, _) = CreateGrain();
        var coordinate = MakeCoordinate(shards: (0, 1));
        var spec = new LatticeCursorSpec { Kind = LatticeCursorKind.Entries, ZeroObservableWrites = true };

        await grain.OpenSnapshotAsync(TreeId, spec, coordinate);
        var pinIdAfterFirst = state.State.SnapshotPinId;

        await grain.OpenSnapshotAsync(TreeId, spec, coordinate);

        Assert.That(state.State.SnapshotPinId, Is.EqualTo(pinIdAfterFirst),
            "Re-open with same coordinate must not rotate the pin id.");
    }

    [Test]
    public async Task OpenSnapshotAsync_rejects_reopen_with_different_coordinate()
    {
        var (grain, _, _) = CreateGrain();
        var spec = new LatticeCursorSpec { Kind = LatticeCursorKind.Keys, ZeroObservableWrites = true };

        await grain.OpenSnapshotAsync(TreeId, spec, MakeCoordinate(shards: (0, 10)));

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.OpenSnapshotAsync(TreeId, spec, MakeCoordinate(shards: (0, 11))));
    }

    [Test]
    public async Task NextKeysAsync_snapshot_mode_does_not_consult_live_lattice()
    {
        // No snapshot-leaf substitutes are registered on the grain
        // factory in this fixture; if the snapshot path attempted a
        // shard fan-out it would NRE on the leaf invocation. The
        // empty-shard-map coordinate exercises the dispatch decision
        // without requiring a real shard activation.
        var (grain, state, lattice) = CreateGrain();
        await grain.OpenSnapshotAsync(
            TreeId,
            new LatticeCursorSpec { Kind = LatticeCursorKind.Keys, ZeroObservableWrites = true },
            MakeCoordinate(shards: System.Array.Empty<(int, long)>()));

        var page = await grain.NextKeysAsync(10);

        Assert.That(page.Keys, Is.Empty);
        Assert.That(page.HasMore, Is.False);
        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Exhausted),
            "Empty snapshot fan-out must mark the cursor exhausted.");
        // The live lattice must never be consulted on the snapshot path.
        lattice.DidNotReceiveWithAnyArgs().KeysAsync(default, default, default);
    }

    [Test]
    public async Task NextEntriesAsync_snapshot_mode_does_not_consult_live_lattice()
    {
        var (grain, state, lattice) = CreateGrain();
        await grain.OpenSnapshotAsync(
            TreeId,
            new LatticeCursorSpec { Kind = LatticeCursorKind.Entries, ZeroObservableWrites = true },
            MakeCoordinate(shards: System.Array.Empty<(int, long)>()));

        var page = await grain.NextEntriesAsync(10);

        Assert.That(page.Entries, Is.Empty);
        Assert.That(page.HasMore, Is.False);
        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.Exhausted));
        lattice.DidNotReceiveWithAnyArgs().EntriesAsync(default, default, default);
    }

    [Test]
    public async Task CloseAsync_clears_snapshot_state()
    {
        var (grain, state, _) = CreateGrain();
        await grain.OpenSnapshotAsync(
            TreeId,
            new LatticeCursorSpec { Kind = LatticeCursorKind.Keys, ZeroObservableWrites = true },
            MakeCoordinate(shards: (0, 5)));

        Assert.That(state.State.SnapshotCoordinate, Is.Not.Null);

        await grain.CloseAsync();

        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.NotStarted),
            "Close clears the persisted state, so the phase resets to its default; " +
            "the in-memory Closed marker is only the fallback when the clear itself fails.");
    }

    // --- Lazy frozen-baseline persist (issue #916) ---

    private static LatticeSnapshotCoordinate MakeTokenCoordinate(
        Guid baselineToken,
        params (int shard, long offset)[] shards)
    {
        var dict = new Dictionary<int, long>();
        foreach (var (s, o) in shards) dict[s] = o;
        return new LatticeSnapshotCoordinate(1, dict, HybridLogicalClock.Zero)
        {
            SnapshotBaselineToken = baselineToken,
        };
    }

    private static (ISnapshotLeafGrain leaf, ISnapshotBaselineStorageGrain baseline) WireSnapshotShard(
        IGrainFactory grainFactory, Guid token, int shardIndex, List<string> keys)
    {
        var key = SnapshotLeafGrain.BuildBaselineKey(TreeId, shardIndex, token);
        var leaf = Substitute.For<ISnapshotLeafGrain>();
        leaf.GetKeysAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<int>(), Arg.Any<LatticePredicateNode?>(), Arg.Any<bool>())
            .Returns(_ => Task.FromResult(new List<string>(keys)));
        grainFactory.GetGrain<ISnapshotLeafGrain>(key).Returns(leaf);

        var baseline = Substitute.For<ISnapshotBaselineStorageGrain>();
        grainFactory.GetGrain<ISnapshotBaselineStorageGrain>(key).Returns(baseline);
        return (leaf, baseline);
    }

    [Test]
    public async Task NextKeysAsync_single_page_does_not_persist_baselines()
    {
        // A snapshot that drains in one page (HasMore == false) must NOT flush
        // the in-memory seed to durable storage - the core write-amplification
        // fix of issue #916.
        var token = Guid.NewGuid();
        var (grain, state, grainFactory) = CreateGrainWithFactory();
        var (leaf, _) = WireSnapshotShard(grainFactory, token, 0, ["a", "b"]);

        await grain.OpenSnapshotAsync(
            TreeId,
            new LatticeCursorSpec { Kind = LatticeCursorKind.Keys, ZeroObservableWrites = true },
            MakeTokenCoordinate(token, (0, 0)));

        var page = await grain.NextKeysAsync(10);

        Assert.That(page.HasMore, Is.False);
        Assert.That(page.Keys, Has.Count.EqualTo(2));
        await leaf.DidNotReceive().EnsurePersistedAsync(Arg.Any<CancellationToken>());
        Assert.That(state.State.SnapshotBaselinePersisted, Is.False,
            "A single-page snapshot must never mark its baselines persisted.");
    }

    [Test]
    public async Task NextKeysAsync_multi_page_persists_baselines_before_returning()
    {
        // The first page returns HasMore == true, so every shard's baseline must
        // be flushed durably BEFORE the continuation token escapes to the client.
        var token = Guid.NewGuid();
        var (grain, state, grainFactory) = CreateGrainWithFactory();
        var (leaf, _) = WireSnapshotShard(grainFactory, token, 0, ["a", "b", "c"]);

        await grain.OpenSnapshotAsync(
            TreeId,
            new LatticeCursorSpec { Kind = LatticeCursorKind.Keys, ZeroObservableWrites = true },
            MakeTokenCoordinate(token, (0, 0)));

        var page = await grain.NextKeysAsync(3);

        Assert.That(page.HasMore, Is.True);
        await leaf.Received(1).EnsurePersistedAsync(Arg.Any<CancellationToken>());
        Assert.That(state.State.SnapshotBaselinePersisted, Is.True,
            "A multi-page snapshot must mark its baselines persisted once flushed.");
    }

    [Test]
    public async Task NextKeysAsync_persists_baselines_only_once_across_pages()
    {
        // The persist fan-out is gated on the SnapshotBaselinePersisted flag, so
        // a second HasMore page must not re-flush.
        var token = Guid.NewGuid();
        var (grain, _, grainFactory) = CreateGrainWithFactory();
        var (leaf, _) = WireSnapshotShard(grainFactory, token, 0, ["a", "b", "c"]);

        await grain.OpenSnapshotAsync(
            TreeId,
            new LatticeCursorSpec { Kind = LatticeCursorKind.Keys, ZeroObservableWrites = true },
            MakeTokenCoordinate(token, (0, 0)));

        await grain.NextKeysAsync(3);
        await grain.NextKeysAsync(3);

        await leaf.Received(1).EnsurePersistedAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CloseAsync_single_page_skips_durable_baseline_delete()
    {
        // Nothing was persisted, so the close path must issue zero ClearAsync
        // calls - no delete-amplification to mirror the absent write.
        var token = Guid.NewGuid();
        var (grain, _, grainFactory) = CreateGrainWithFactory();
        var (_, baseline) = WireSnapshotShard(grainFactory, token, 0, ["a", "b"]);

        await grain.OpenSnapshotAsync(
            TreeId,
            new LatticeCursorSpec { Kind = LatticeCursorKind.Keys, ZeroObservableWrites = true },
            MakeTokenCoordinate(token, (0, 0)));
        await grain.NextKeysAsync(10);

        await grain.CloseAsync();

        await baseline.DidNotReceive().ClearAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CloseAsync_multi_page_deletes_persisted_baselines()
    {
        // A cursor that flushed its baselines must clean them up on close.
        var token = Guid.NewGuid();
        var (grain, _, grainFactory) = CreateGrainWithFactory();
        var (_, baseline) = WireSnapshotShard(grainFactory, token, 0, ["a", "b", "c"]);

        await grain.OpenSnapshotAsync(
            TreeId,
            new LatticeCursorSpec { Kind = LatticeCursorKind.Keys, ZeroObservableWrites = true },
            MakeTokenCoordinate(token, (0, 0)));
        await grain.NextKeysAsync(3);

        await grain.CloseAsync();

        await baseline.Received(1).ClearAsync(Arg.Any<CancellationToken>());
    }
}

