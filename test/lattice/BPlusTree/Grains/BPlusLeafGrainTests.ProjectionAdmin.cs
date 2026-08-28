using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the operator-tooling seams on
/// <see cref="BPlusLeafGrain"/>: the read-only checkpoint accessor
/// (<see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.GetProjectionCheckpointOffsetAsync"/>)
/// and the destructive projection-rebuild path
/// (<see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.RebuildProjectionFromWalAsync"/>).
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task GetProjectionCheckpointOffsetAsync_returns_zero_on_fresh_grain()
    {
        var grain = CreateGrain();
        var offset = await grain.GetProjectionCheckpointOffsetAsync();
        Assert.That(offset, Is.EqualTo(0L));
    }

    [Test]
    public async Task GetProjectionCheckpointOffsetAsync_returns_persisted_value()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.ProjectionCheckpointOffset = 42L;
        var grain = CreateGrain(state);

        var offset = await grain.GetProjectionCheckpointOffsetAsync();
        Assert.That(offset, Is.EqualTo(42L));
    }

    [Test]
    public async Task RebuildProjectionFromWalAsync_clears_entries_and_resets_checkpoint()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Seed the leaf via the foreground path so Entries, the
        // projection hash, and the persisted checkpoint are all
        // populated.
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));
        state.State.ProjectionCheckpointOffset = 17L;
        state.State.ProjectionHash = new byte[16];
        Array.Fill(state.State.ProjectionHash, (byte)0xAB);

        await grain.RebuildProjectionFromWalAsync();

        Assert.That(grain.EntriesForTest, Is.Empty);
        // Rebuild persists the "nothing applied" sentinel (-1), NOT 0,
        // because the materialiser reads strictly past the checkpoint
        // and would otherwise skip WAL offset 0 on the next activation.
        // See LatticeFallOffLogDetectorTests and
        // BPlusLeafGrainTests.Materialiser_replays_offset_zero_when_checkpoint_is_nothing_applied_sentinel
        // for the matching coverage of the sentinel contract.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(-1L));
        Assert.That(state.State.ProjectionHash, Is.Null);
    }

    [Test]
    public async Task RebuildProjectionFromWalAsync_preserves_topology_state()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "fixed-tree";
        state.State.ShardIndex = 3;
        state.State.LowKeyInclusive = "alpha";
        state.State.HighKeyExclusive = "omega";
        var grain = CreateGrain(state);

        await grain.SetAsync("kx", Encoding.UTF8.GetBytes("vx"));

        await grain.RebuildProjectionFromWalAsync();

        Assert.That(state.State.TreeId, Is.EqualTo("fixed-tree"));
        Assert.That(state.State.ShardIndex, Is.EqualTo(3));
        Assert.That(state.State.LowKeyInclusive, Is.EqualTo("alpha"));
        Assert.That(state.State.HighKeyExclusive, Is.EqualTo("omega"));
    }

    [Test]
    public async Task RebuildProjectionFromWalAsync_persists_cleared_state()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        var writesBefore = state.WriteCount;
        await grain.RebuildProjectionFromWalAsync();

        // The rebuild persists exactly once via PersistAsync to
        // commit the cleared projection slots.
        Assert.That(state.WriteCount, Is.GreaterThan(writesBefore));
    }

    [Test]
    public async Task RebuildProjectionFromWalAsync_is_idempotent_when_already_empty()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Nothing was ever written; a rebuild on a fresh leaf must
        // be a benign no-op (the projection is already empty). The
        // persisted checkpoint flips from the freshly persisted default
        // (0) to the "nothing applied" sentinel (-1) so the next
        // activation replays from WAL offset 0 inclusive.
        Assert.That(async () => await grain.RebuildProjectionFromWalAsync(), Throws.Nothing);
        Assert.That(grain.EntriesForTest, Is.Empty);
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(-1L));
    }
}
