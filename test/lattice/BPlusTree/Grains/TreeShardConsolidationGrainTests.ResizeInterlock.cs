using NSubstitute;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4452, consolidation side: an online
/// consolidation opens its donor's shadow-write window through the same
/// migration primitive a split uses, so it is interlocked with a resize the
/// same way.
/// </summary>
public partial class TreeShardConsolidationGrainTests
{
    [Test]
    public void StartAsync_refuses_while_a_completed_resize_still_has_the_replaced_copy_mirroring()
    {
        var h = CreateGrain();
        h.Factory.StubResizeIdle().HoldsShardMigrationsAsync().Returns(Task.FromResult(true));

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.StartAsync(0));

        Assert.That(h.Log.Entries, Does.Not.Contain("donor.BeginSplit"));
        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public async Task StartAsync_refuses_while_a_resize_of_the_tree_is_in_flight()
    {
        var h = CreateGrain();
        h.Factory.StubResizeIdle().HoldsShardMigrationsAsync().Returns(Task.FromResult(true));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.StartAsync(0));

        Assert.That(ex!.Message, Does.Contain("resize of the tree is in progress"));
        Assert.That(h.Log.Entries, Does.Not.Contain("donor.BeginSplit"));
        Assert.That(h.State.State.InProgress, Is.False);
        await Task.CompletedTask;
    }

    [Test]
    public void Initiate_backs_out_when_a_resize_is_in_flight_once_the_donor_record_is_open()
    {
        // Idle at the pre-check, in flight at the read after the donor's record
        // opens: the race only the second read can close.
        var h = CreateGrain();
        h.Factory.StubResizeIdle().HoldsShardMigrationsAsync().Returns(Task.FromResult(false), Task.FromResult(true));

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.InitiateConsolidationStateAsync(1, 0));

        Assert.That(h.Log.Entries, Is.EqualTo(new[] { "donor.BeginSplit", "donor.AbortSplit" }));
        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public async Task A_fold_resumed_before_its_drain_abandons_when_a_resize_is_in_flight()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.BeginShadowWrite));
        h.Factory.StubResizeIdle().HoldsShardMigrationsAsync().Returns(Task.FromResult(true));

        await h.Grain.ReopenShadowWriteAsync();

        Assert.That(h.Log.Entries, Is.EqualTo(new[] { "donor.BeginSplit", "donor.AbortSplit" }));
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.InProgress, Is.False);
            Assert.That(h.State.State.Cancelled, Is.False);
            Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.None));
        });
    }
}
