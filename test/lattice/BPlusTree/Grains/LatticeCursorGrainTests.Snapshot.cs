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

        Assert.That(state.State.Phase, Is.EqualTo(LatticeCursorPhase.NotStarted)
            .Or.EqualTo(LatticeCursorPhase.Closed),
            "Close must clear or close the persisted phase.");
    }
}

