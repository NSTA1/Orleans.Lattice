using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Applier-level coverage for the restored-copy receive fence (issue #4593): an
/// apply that routed to a restored copy a coordinated restore still holds closed
/// must surface as <see cref="ApplyResult.Deferred"/>, so the sender re-ships it
/// once the restore's fence lifts - never as a failure, a dead letter, or a
/// silent acknowledgement.
/// </summary>
public partial class ReplicationApplierTests
{
    private static CopyReceiveFencedException CopyFenced() => new(Tree, "tree-restored");

    [Test]
    public async Task ApplyAsync_defers_an_entry_that_routed_to_a_closed_restored_copy()
    {
        var (applier, apply) = CreateApplierOverApply();
        apply.ApplySetAsync(default!, default!, default, default!, default, default)
            .ReturnsForAnyArgs(Task.FromException(CopyFenced()), Task.CompletedTask);
        var entry = SetEntry("k", Hlc(10));

        var deferred = await applier.ApplyAsync(entry);
        var redelivered = await applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(deferred.Deferred, Is.True);
            Assert.That(deferred.Applied, Is.False);
            Assert.That(redelivered.Applied, Is.True, "the deferral releases the dedupe reservation so the re-ship applies");
        });
    }

    [Test]
    public async Task ApplyBatchAsync_defers_a_run_that_routed_to_a_closed_restored_copy()
    {
        var (applier, apply) = CreateApplierOverApply();
        apply.ApplyMergeManyAsync(default!).ReturnsForAnyArgs(Task.FromException(CopyFenced()));
        apply.ApplySetAsync(default!, default!, default, default!, default, default)
            .ReturnsForAnyArgs(Task.FromException(CopyFenced()));

        var result = await applier.ApplyBatchAsync(new[]
        {
            SetEntry("a", Hlc(10)),
            SetEntry("b", Hlc(20)),
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Deferred, Is.True);
            Assert.That(result.Applied, Is.False);
        });
    }

    [Test]
    public async Task Causal_buffer_drain_leaves_an_entry_for_a_closed_restored_copy_parked()
    {
        var h = CreateCausalHarness();
        await h.Applier.ApplyAsync(BlockedOnSiteC("k", 100));
        Assert.That(h.BufferState.State.Entries, Has.Count.EqualTo(1));
        h.Vc.Entries[OriginC] = Hlc(50);
        h.Apply.ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(100), RemoteCluster, null, Arg.Any<long>())
            .Returns(Task.FromException(CopyFenced()), Task.CompletedTask);
        var grain = (CausalApplyBufferGrain)h.Factory.GetGrain<ICausalApplyBufferGrain>(Tree);

        var whileClosed = await grain.DrainAsync();
        var afterOpen = await grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(whileClosed, Is.EqualTo(1), "an entry for a closed copy stays parked, not dead-lettered");
            Assert.That(afterOpen, Is.Zero);
        });
    }
}
