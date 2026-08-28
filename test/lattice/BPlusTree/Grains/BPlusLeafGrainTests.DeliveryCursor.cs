using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.BPlusTree.State;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regressions for the delivery cursor's cross-process epoch contract.
/// <para>
/// The epoch identifies one leaf activation and is compared by a cache that
/// can outlive the silo which minted it. Two guarantees are pinned here: the
/// per-process seed is randomised so two silos do not hand out the same low
/// integers, and a cursor whose sequence is ahead of the activation's is
/// rejected as stale even when the epoch matches, so a residual collision
/// still fails safe into the full-snapshot path instead of silently starving
/// the cache of the resync.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task GetDeltaSinceCursorAsync_cursor_sequence_ahead_of_activation_returns_full_snapshot()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        // Adopt the activation's own cursor, then forge one that shares the
        // epoch but claims a sequence this activation cannot have issued -
        // exactly the shape a cross-process epoch collision produces, where a
        // surviving cache presents a high sequence from the previous
        // activation against a fresh activation that restarted at zero.
        var current = (await grain.GetDeltaSinceCursorAsync(LeafDeliveryCursor.Empty)).DeliveryCursor;
        var forged = current with { Sequence = current.Sequence + 1_000 };

        var delta = await grain.GetDeltaSinceCursorAsync(forged);

        Assert.Multiple(() =>
        {
            // Without the guard this returns an empty delta forever, because
            // the epoch matches and the caller's sequence is already at or
            // beyond the activation's head.
            Assert.That(delta.IsEmpty, Is.False, "a stale cursor must be answered with a snapshot, not an empty delta");
            Assert.That(delta.Entries, Has.Count.EqualTo(2));
            Assert.That(delta.Entries.ContainsKey("a"), Is.True);
            Assert.That(delta.Entries.ContainsKey("b"), Is.True);
            Assert.That(delta.DeliveryCursor.Sequence, Is.EqualTo(current.Sequence),
                "the snapshot re-anchors the caller on this activation's real head");
        });
    }

    [Test]
    public async Task GetDeltaSinceCursorAsync_cursor_at_head_still_returns_empty_delta()
    {
        // The guard must not fire on the ordinary steady-state case: an equal
        // sequence is legitimately "already at head" and must stay cheap.
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var current = (await grain.GetDeltaSinceCursorAsync(LeafDeliveryCursor.Empty)).DeliveryCursor;
        var delta = await grain.GetDeltaSinceCursorAsync(current);

        Assert.That(delta.IsEmpty, Is.True);
    }

    [Test]
    public async Task GetDeltaSinceCursorAsync_incremental_delivery_still_ships_only_newer_entries()
    {
        // The guard must not disturb the incremental path either.
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var afterFirst = (await grain.GetDeltaSinceCursorAsync(LeafDeliveryCursor.Empty)).DeliveryCursor;
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var delta = await grain.GetDeltaSinceCursorAsync(afterFirst);

        Assert.Multiple(() =>
        {
            Assert.That(delta.Entries, Has.Count.EqualTo(1));
            Assert.That(delta.Entries.ContainsKey("b"), Is.True);
        });
    }

    [Test]
    public async Task GetDeltaSinceCursorAsync_epoch_is_not_seeded_from_zero()
    {
        // The defect: a seed starting at zero makes every silo hand out
        // 1, 2, 3, ... so two processes collide on the low integers. A
        // randomised seed cannot be asserted exactly, but it can be asserted
        // not to be one of the handful of values a zero-seeded counter would
        // produce for the first activations of a process.
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var cursor = (await grain.GetDeltaSinceCursorAsync(LeafDeliveryCursor.Empty)).DeliveryCursor;

        Assert.Multiple(() =>
        {
            Assert.That(cursor.Epoch, Is.Not.Zero, "zero is reserved for LeafDeliveryCursor.Empty");
            Assert.That(cursor.Epoch, Is.GreaterThan(1_000L),
                "a zero-seeded per-process counter would mint a low integer here, which collides across silos");
        });
    }

    [Test]
    public async Task GetDeltaSinceCursorAsync_epoch_is_stable_within_one_activation()
    {
        // Randomising the seed must not cost per-activation stability: the
        // whole incremental path depends on the epoch being constant for the
        // life of the activation.
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var first = (await grain.GetDeltaSinceCursorAsync(LeafDeliveryCursor.Empty)).DeliveryCursor;
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        var second = (await grain.GetDeltaSinceCursorAsync(first)).DeliveryCursor;

        Assert.That(second.Epoch, Is.EqualTo(first.Epoch));
    }

    [Test]
    public async Task GetDeltaSinceCursorAsync_distinct_activations_mint_distinct_epochs()
    {
        // Monotonicity within the process is what keeps two activations on the
        // same silo distinguishable; the randomised seed only moves the
        // starting point.
        var first = CreateGrain();
        await first.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        var firstCursor = (await first.GetDeltaSinceCursorAsync(LeafDeliveryCursor.Empty)).DeliveryCursor;

        var second = CreateGrain(new FakePersistentState<LeafNodeState>());
        await second.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        var secondCursor = (await second.GetDeltaSinceCursorAsync(LeafDeliveryCursor.Empty)).DeliveryCursor;

        Assert.That(secondCursor.Epoch, Is.Not.EqualTo(firstCursor.Epoch));
    }
}
