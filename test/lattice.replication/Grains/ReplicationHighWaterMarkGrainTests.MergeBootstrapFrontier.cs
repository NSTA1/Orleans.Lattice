using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// #4464 mechanism 3: the bootstrap pin merges its frontier with the vector
/// already held (pointwise maximum) instead of replacing it, so a receiver that
/// already applied an origin's writes above the source's frontier does not move
/// backwards. The restore re-seed keeps <c>PinSnapshotAsync</c>'s replace.
/// </summary>
public partial class ReplicationHighWaterMarkGrainTests
{
    [Test]
    public async Task MergeBootstrapFrontierAsync_takes_the_pointwise_maximum_and_never_regresses()
    {
        var grain = CreateGrain();
        await grain.TryAdvanceAsync(OriginA, Hlc(10), CancellationToken.None);

        var raised = await grain.MergeBootstrapFrontierAsync(
            Hlc(1), Vector((OriginA, Hlc(5)), (OriginB, Hlc(7))), CancellationToken.None);

        var vector = await grain.GetVectorAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.True);
            Assert.That(vector.GetClock(OriginA), Is.EqualTo(Hlc(10)), "Already above the frontier: kept.");
            Assert.That(vector.GetClock(OriginB), Is.EqualTo(Hlc(7)), "Below the frontier: raised.");
        });
    }

    [Test]
    public async Task MergeBootstrapFrontierAsync_does_not_write_when_nothing_rises()
    {
        var state = new FakePersistentState<Replication.Grains.ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.TryAdvanceAsync(OriginA, Hlc(10), CancellationToken.None);
        var writes = state.WriteCount;

        var raised = await grain.MergeBootstrapFrontierAsync(Hlc(1), Vector((OriginA, Hlc(5))), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.False);
            Assert.That(state.WriteCount, Is.EqualTo(writes));
        });
    }

    [Test]
    public async Task MergeBootstrapFrontierAsync_clears_a_legacy_drop_floor()
    {
        var state = new FakePersistentState<Replication.Grains.ReplicationHighWaterMarkState>();
        state.State.PinnedFloor.Entries[OriginA] = Hlc(9);
        var grain = CreateGrain(state);

        await grain.MergeBootstrapFrontierAsync(Hlc(1), Vector((OriginA, Hlc(5))), CancellationToken.None);

        Assert.That(await grain.GetPinnedFloorAsync(OriginA, CancellationToken.None), Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task MergeBootstrapFrontierAsync_rolls_back_on_storage_failure()
    {
        var state = new FakePersistentState<Replication.Grains.ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.TryAdvanceAsync(OriginA, Hlc(3), CancellationToken.None);
        state.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await grain.MergeBootstrapFrontierAsync(Hlc(1), Vector((OriginA, Hlc(5))), CancellationToken.None));

        Assert.That(await grain.GetAsync(OriginA, CancellationToken.None), Is.EqualTo(Hlc(3)));
    }

    [Test]
    public async Task PinSnapshotAsync_still_replaces_for_the_restore_re_seed()
    {
        var grain = CreateGrain();
        await grain.TryAdvanceAsync(OriginA, Hlc(10), CancellationToken.None);

        await grain.PinSnapshotAsync(HybridLogicalClock.Zero, Vector((OriginA, Hlc(5))), CancellationToken.None);

        Assert.That(await grain.GetAsync(OriginA, CancellationToken.None), Is.EqualTo(Hlc(5)));
    }
}
